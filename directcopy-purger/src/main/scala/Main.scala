import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import akka.stream.{ActorMaterializer, ClosedShape, SourceShape}
import akka.stream.scaladsl.{Broadcast, GraphDSL, RunnableGraph, Sink}
import streamcomponents.{FileListSource, FilterOutDirectories, FilterOutMacStuff, FindRemoteFile, LocalChecksum, RemoteLocalMerger, UTF8PathCharset, ValidateAndDelete}
import akka.stream.scaladsl.GraphDSL.Implicits._
import com.om.mxs.client.japi.UserInfo
import helpers.{PlutoCommunicator, TrustStoreHelper}
import models.{FileEntry, PathTransform, PathTransformSet}
import org.slf4j.LoggerFactory

import java.io.{File, FileInputStream}
import java.nio.file.{Path, Paths}
import java.util.Properties
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  lazy implicit val actorSystem = ActorSystem("direct-copy")
  lazy implicit val mat = ActorMaterializer()
  lazy implicit val ec:ExecutionContext = actorSystem.dispatcher

  lazy val copyChunkSize = sys.env.get("CHUNK_SIZE").map(_.toInt)
  lazy val parallelCopies = sys.env.get("PARALLEL_COPIES").map(_.toInt).getOrElse(1)
  lazy val copyLimit = sys.env.get("LIMIT").map(_.toInt)
  lazy val startingPath = Paths.get(requiredEnvironment("START_PATH"))

  val maybePathStrip = sys.env.get("PATH_STRIP").map(_.toInt)

  val pathTransformList = new PathTransformSet(
    sys.env.get("PATH_TRANSFORM_LIST")
      .map(_.split("\\|"))
      .map(_.map(PathTransform.fromPathSpec(_, maybePathStrip)))
      .map(_.map({
        case Right(transform)=>transform
        case Left(err)=>
          logger.error(s"Could not make a path transform out of ${sys.env.get("PATH_TRANSFORM")}: $err")
          sys.exit(1)
      }))
      .map(_.toSeq)
      .getOrElse(Seq())
  )

  lazy val plutoCredentialsFile = requiredEnvironment("PLUTO_CREDENTIALS_FILE")
  def getPlutoCommunicator:Try[ActorRef] = {
    val propsFile = Try {
      val prop = new Properties()

      val f = new File(plutoCredentialsFile)
      val strm = new FileInputStream(f)
      prop.load(strm)
      strm.close()
      prop
    }

    propsFile.flatMap(properties=>{
      val baseUri = Option(properties.getProperty("base-uri"))
      val sharedSecret = Option(properties.getProperty("shared-secret"))

      if(baseUri.isEmpty || sharedSecret.isEmpty){
        Failure(new RuntimeException("Invalid properties. You must provide base-uri, user and password properties for pluto access"))
      } else {
        Success(actorSystem.actorOf(Props(new PlutoCommunicator(baseUri.get, sharedSecret.get))))
      }
    })
  }

  /**
    * helper method that either gets the requested key from the environment or exits with an indicative
    * error if it does not exist
    * @param key environment variable name to get
    * @return the value of the variable, if it exists. If it does not exist then the method does not return.
    */
  private def requiredEnvironment(key:String):String = sys.env.get(key) match {
    case Some(value)=>value
    case None=>
      logger.error(s"You must specify $key in the environment")
      sys.exit(1)
  }

  lazy val destVaultInfo =
    UserInfoBuilder.fromFile(requiredEnvironment("DEST_VAULT")).get //allow this to crash if we can't load the file, traceback will explain why

  val sourceMediaPath = Paths.get(requiredEnvironment("SOURCE_MEDIA_PATH"))
  val proxyMediaPath = Paths.get(requiredEnvironment("PROXY_MEDIA_PATH"))
  val proxyMediaPostfix = sys.env.get("PROXY_MEDIA_POSTFIX")
  val proxyMediaXtnList = requiredEnvironment("PROXY_MEDIA_XTN").split(",").toList
  val thumbnailPostfix = sys.env.get("THUMBNAIL_POSTFIX")
  val thumbnailXtn = requiredEnvironment("THUMBNAIL_XTN")

  lazy val extraKeyStores = sys.env.get("EXTRA_KEY_STORES").map(_.split("\\s*,\\s*"))

  /**
    * partial akka stream that lists out all of the source files we are interested in
    * @param startingPath
    * @return
    */
  private def inputStream(startingPath:Path) =
    GraphDSL.create() { implicit builder=>
      val src = builder.add(FileListSource(startingPath))
      val pathFixer = builder.add(new UTF8PathCharset)
      val dirFilter = builder.add(new FilterOutDirectories)
      val macStuffFilter = builder.add(new FilterOutMacStuff)

      src ~> pathFixer ~> dirFilter ~> macStuffFilter
      copyLimit match {
        case None=>
          SourceShape(macStuffFilter.out)
        case Some(limit)=>
          SourceShape(macStuffFilter.take(limit).outlet)
      }
    }

  /**
    * main akka stream that puts all the pieces together and manages media copying
    * @param startingPath path at which to start recursive scan
    * @return a Graph, which resolves to a Future[Done] which completes when all processing is done
    */
  def buildStream(startingPath:Path, userInfo:UserInfo, reallyDelete:Boolean) = {
    val sinkFac = new ValidateAndDelete(reallyDelete)

    GraphDSL.create(sinkFac) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(inputStream(startingPath))
      val splitter = builder.add(Broadcast[FileEntry](2,false))
      val localChecksummer = builder.add((new LocalChecksum()).async)
      val remoteFinder = builder.add(new FindRemoteFile(userInfo).async)
      val matcher = builder.add(new RemoteLocalMerger())

      src.out.map(FileEntry.fromPath).collect({ case Success(entry)=>entry }) ~> splitter
      splitter.out(0) ~> localChecksummer ~> matcher.in(0)
      splitter.out(1) ~> remoteFinder ~> matcher.in(1)

      matcher.out ~> sink

      ClosedShape
    }
  }

  /**
    * shuts down the actor system then exits the JVM.
    * @param exitCode
    * @return
    */
  def terminate(exitCode:Int) = {
    import scala.concurrent.duration._

    Await.ready(actorSystem.terminate(), 1 minute)
    sys.exit(exitCode)
  }



  def main(args:Array[String]) = {
    if(extraKeyStores.isDefined){
      logger.info(s"Loading in extra keystores from ${extraKeyStores.get.mkString(",")}")
      /* this should set the default SSL context to use the stores as well */
      TrustStoreHelper.setupTS(extraKeyStores.get) match {
        case Success(sslContext) =>
          val https: HttpsConnectionContext = ConnectionContext.https(sslContext)
          Http().setDefaultClientHttpsContext(https)
        case Failure(err) =>
          logger.error(s"Could not set up https certs: ", err)
          System.exit(4)
      }
    }

  }
}

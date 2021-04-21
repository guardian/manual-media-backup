import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, SourceShape}
import akka.stream.scaladsl.{GraphDSL, Sink}
import streamcomponents.{FileListSource, FilterOutDirectories, FilterOutMacStuff, LocateProxyFlow, UTF8PathCharset}
import akka.stream.scaladsl.GraphDSL.Implicits._
import helpers.Copier
import models.ToCopy
import org.slf4j.LoggerFactory

import java.nio.file.{Path, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.io.File
import scala.util.{Failure, Success}

object directcopy {
  private val logger = LoggerFactory.getLogger(getClass)

  lazy implicit val actorSystem = ActorSystem("direct-copy")
  lazy implicit val mat = ActorMaterializer()
  lazy implicit val ec:ExecutionContext = actorSystem.dispatcher

  lazy val copyChunkSize = sys.env.get("CHUNK_SIZE").map(_.toInt)

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
  val proxyMediaXtn = requiredEnvironment("PROXY_MEDIA_XTN")
  val thumbnailPostfix = sys.env.get("THUMBNAIL_POSTFIX")
  val thumbnailXtn = requiredEnvironment("THUMBNAIL_XTN")

  /**
    * partial akka stream that lists out all of the source files we are interested in
    * @param startingPath
    * @return
    */
  def inputStream(startingPath:Path) =
    GraphDSL.create() { implicit builder=>
      val src = builder.add(FileListSource(startingPath))
      val pathFixer = builder.add(new UTF8PathCharset)
      val dirFilter = builder.add(new FilterOutDirectories)
      val macStuffFilter = builder.add(new FilterOutMacStuff)

      src ~> pathFixer ~> dirFilter ~> macStuffFilter
      SourceShape(macStuffFilter.out)
    }

//  def buildStream(startingPath:Path, copier:DirectCopier) = {
//    val sinkFac = Sink.ignore
//    GraphDSL.create(sinkFac) { implicit builder=>
//      val src = inputStream(startingPath)
//      val proxyLocator = builder.add(new LocateProxyFlow(sourceMediaPath, proxyMediaPath, proxyMediaPostfix, proxyMediaXtn, thumbnailPostfix, thumbnailXtn))
//
//      src ~> proxyLocator
//      ClosedShape
//    }
//  }

  def main(args:Array[String]) = {
    DirectCopier.initialise(destVaultInfo) match {
      case Success(copier)=>

      case Failure(err)=>
        logger.error(s"Could not access vault given in DEST_VAULT: ${err.toString}", err)
        sys.exit(2)
    }
  }
}

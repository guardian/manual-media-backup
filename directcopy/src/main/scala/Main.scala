import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, ClosedShape, SourceShape}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink}
import streamcomponents.{FileListSource, FilterOutDirectories, FilterOutMacStuff, LocateProxyFlow, UTF8PathCharset}
import akka.stream.scaladsl.GraphDSL.Implicits._
import helpers.PlutoCommunicator
import models.PathTransform
import org.slf4j.LoggerFactory

import java.nio.file.{Path, Paths}
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

  val pathTransformList = sys.env.get("PATH_TRANSFORM_LIST")
    .map(_.split("\\|"))
    .map(_.map(PathTransform.fromPathSpec(_, maybePathStrip)))
    .map(_.map({
      case Right(transform)=>transform
      case Left(err)=>
        logger.error(s"Could not make a path transform out of ${sys.env.get("PATH_TRANSFORM")}: $err")
        sys.exit(1)
    }))
    .getOrElse(Array())

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

  val plutoBaseUri = requiredEnvironment("PLUTO_BASE_URI")
  val plutoSharedSecret = requiredEnvironment("PLUTO_SHARED_SECRET")

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
    * @param copier [[DirectCopier]] instance
    * @return a Graph, which resolves to a Future[Done] which completes when all processing is done
    */
  def buildStream(startingPath:Path, copier:DirectCopier, plutoMetadataTool: PlutoMetadataTool) = {
    val sinkFac = Sink.ignore
    GraphDSL.create(sinkFac) { implicit builder=> sink=>
      val src = builder.add(inputStream(startingPath))
      val proxyLocator = builder.add(new LocateProxyFlow(sourceMediaPath, proxyMediaPath, proxyMediaPostfix, proxyMediaXtnList, thumbnailPostfix, thumbnailXtn))

      src ~> proxyLocator
      proxyLocator
        .mapAsync(parallelCopies)(plutoMetadataTool.addPlutoMetadata)
        .mapAsync(parallelCopies)(copier.performCopy(_,copyChunkSize))
        .map(copiedData=> {
          logger.info(s"Completed copy of ${copiedData.sourceFile.path.toString} to ${copiedData.sourceFile.oid} with checksum ${copiedData.sourceFile.oid}")
          copiedData.proxyMedia match {
            case None=>
              logger.info(s"No proxy found for ${copiedData.sourceFile.path.toString}")
            case Some(proxyMedia)=>
              logger.info(s"Completed copy of proxy ${proxyMedia.path} to ${proxyMedia.oid} with checksum ${proxyMedia.omChecksum}")
          }
          copiedData.thumbnail match {
            case None=>
              logger.info(s"No thumbnail found for ${copiedData.sourceFile.path.toString}")
            case Some(thumbnail)=>
              logger.info(s"Completed copy of thumbnail ${thumbnail.path} to ${thumbnail.oid} with checksum ${thumbnail.omChecksum}")
          }
          copiedData
        }) ~> sink
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
    import akka.pattern.ask
    import scala.concurrent.duration._
    implicit val timeout:akka.util.Timeout = 10.seconds

    DirectCopier.initialise(destVaultInfo, pathTransformList) match {
      case Success(copier)=>
        logger.info("Connected to vault")
        if(!startingPath.toFile.exists()) {
          logger.error(s"Starting file path ${startingPath.toString} does not exist, can't continue")
          sys.exit(1)
        }
        val plutoCommunicator = actorSystem.actorOf(Props(new PlutoCommunicator(plutoBaseUri, plutoSharedSecret)))
        Try {
          Await.result((plutoCommunicator ? PlutoCommunicator.TestConnection).mapTo[PlutoCommunicator.AFHMsg], 10.seconds)
        } match {
          case Success(PlutoCommunicator.ConnectionWorking)=>
            logger.info("Successfully connected to pluto")
          case Success(PlutoCommunicator.LookupFailed)=>
            logger.error("Unable to connect to pluto, please examine the logs above")
            terminate(3)
          case Failure(err)=>
            logger.error(s"Test connection to pluto failed: ${err.getMessage}", err)
            terminate(3)
        }

        val mdTool = new PlutoMetadataTool(plutoCommunicator)
        val graph = buildStream(startingPath, copier, mdTool)

        RunnableGraph.fromGraph(graph).run().onComplete({
          case Failure(err)=>
            logger.error(s"Copy run failed: ${err.getMessage}", err)
          case Success(_)=>
            logger.info("Copy run completed successfully.")
            terminate(0)
        })
      case Failure(err)=>
        logger.error(s"Could not access vault given in DEST_VAULT: ${err.toString}", err)
        sys.exit(2)
    }
  }
}

package streamcomponents

import akka.Done
import akka.stream.{Attributes, FlowShape, Inlet, Outlet, SinkShape, UniformFanOutShape}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic, GraphStageWithMaterializedValue}
import models.FileEntry
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class ValidateAndDelete(reallyDelete:Boolean) extends GraphStageWithMaterializedValue[SinkShape[FileEntry], Future[Done]] {
  private final val logger = LoggerFactory.getLogger(getClass)
  private final val in:Inlet[FileEntry] = Inlet.create("Validator.in")

  override def shape: SinkShape[FileEntry] = SinkShape.of(in)

  def doDelete(filePath:Path) = Try { Files.delete(filePath) }

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val completionPromise = Promise[Done]()

    val logic = new GraphStageLogic(shape) {
      override def preStart(): Unit = {
        if(!hasBeenPulled(in)) pull(in)
      }

      setHandler(in, new AbstractInHandler {
        override def onPush(): Unit = {
          val elem = grab(in)

          if(elem.remoteFile.isEmpty) {
            logger.error(s"${elem.localFile.filePath.toString} had no remote file, this should not happen")
            val err = new RuntimeException("No remote file present for checking")
            //completionPromise.failure(err)
            pull(in)
          } else if(elem.localFile.length!=elem.remoteFile.get.length) {
            logger.error(s"${elem.localFile.filePath.toString} file size mismatch: ${elem.localFile.length} local vs ${elem.remoteFile.get.length} remote")
            pull(in)
          } else if(elem.localFile.md5!=elem.remoteFile.get.md5) {
            logger.error(s"${elem.localFile.filePath.toString} file checksum mismatch ${elem.localFile.md5} local bs ${elem.remoteFile.get.md5} remote")
            pull(in)
          } else {
            logger.info(s"${elem.localFile.filePath.toString} copy verified")
            if(reallyDelete) {
              doDelete(elem.localFile.filePath) match {
                case Success(_)=>logger.info(s"Deleted ${elem.localFile.filePath}")
                case Failure(err)=>logger.error(s"Could not delete ${elem.localFile.filePath}: $err")
              }
            } else {
              logger.info("Not deleting anything until reallyDelete is set")
            }
            pull(in)
          }
        }

        override def onUpstreamFinish(): Unit = {
          completionPromise.success(Done)
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          completionPromise.failure(ex)
        }
      })
    }
    (logic, completionPromise.future)
  }
}

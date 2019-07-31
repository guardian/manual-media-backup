package vsStreamComponents

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.{HttpError, VSBackupEntry}
import org.slf4j.LoggerFactory
import vidispine.{VSCommunicator, VSFile}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * graph stage that gets VS file metadata and updates the flow element with it
  * @param comm VSCommunicator instance, set up with VS credentials
  * @param mat implicitly provided materializer
  */
class LookupVidispineMD5(comm:VSCommunicator)(implicit mat:Materializer) extends GraphStage[FlowShape[VSBackupEntry,VSBackupEntry]] {
  private final val in:Inlet[VSBackupEntry] = Inlet.create("LookupVidispineMD5.in")
  private final val out:Outlet[VSBackupEntry] = Outlet.create("LookupVidispineMD5.out")
  private val outerLogger = LoggerFactory.getLogger(getClass)

  override def shape: FlowShape[VSBackupEntry, VSBackupEntry] = FlowShape.of(in,out)

  /**
    * try to look up information about the given file in VS.
    * loop and retry if a 503 or a 504 is returned
    * @param storageId
    * @param fileId
    * @param attempt
    * @return
    */
  def lookupWithRetry(storageId:String,fileId:String, attempt:Int=1):Future[Either[HttpError,VSFile]] = {
    comm.requestGet(s"/API/storage/$storageId/file/$fileId", headers=Map("Content-Type"->"application/xml")).flatMap({
      case Left(err)=>
        if(err.errorCode==503 || err.errorCode==504){
          outerLogger.warn(s"Vidispine not available on attempt $attempt")
          Thread.sleep(2000)
          lookupWithRetry(storageId, fileId, attempt+1)
        } else {
          outerLogger.warn(s"Vidispine could not return data: $err")
          Future(Left(err))
        }
      case Right(xmlString)=>VSFile.fromXmlString(xmlString) match {
        case Failure(err)=>
          outerLogger.error(s"Could not parse data from server", err)
          Future(Left(HttpError("Could not parse data",0)))
        case Success(None)=>
          outerLogger.error(s"No files returned")
          Future(Left(HttpError("No files returned",0)))
        case Success(Some(vsFile))=>
          Future(Right(vsFile))
      }
    })
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val completedCb = createAsyncCallback[VSBackupEntry](entry=>push(out,entry))
        val failedCb = createAsyncCallback[Throwable](err=>fail(out,err))
        val ignoreCb = createAsyncCallback[Unit](_=>pull(in))

        lookupWithRetry(elem.sourceStorage.get,elem.vsFileId.get).map({
          case Left(err)=>
            logger.error(s"Could not look up vs item: $err")
            failedCb.invoke(new RuntimeException("Could not look up VS item"))
          case Right(vSFile)=>
            val updatedElem = elem.copy(vidispineMD5 = vSFile.hash, vidispineSize = Some(vSFile.size))
            completedCb.invoke(updatedElem)
        }).recover({
          case err:Throwable=>
            logger.error("lookupWithRetry crashed", err)
            failedCb.invoke(err)
        })
      }
    })
  }
}

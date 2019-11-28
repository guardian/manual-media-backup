package vsStreamComponents

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.{CopyReport, HttpError, VSBackupEntry}
import org.slf4j.LoggerFactory
import vidispine.VSCommunicator

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * sets the newly created file to the "CLOSED" state
  */
class VSCloseFile(comm:VSCommunicator,storageId:String)(implicit mat:Materializer) extends GraphStage[FlowShape[CopyReport[VSBackupEntry],CopyReport[VSBackupEntry]]] {
  private final val in:Inlet[CopyReport[VSBackupEntry]] = Inlet.create("VSCloseFile.in")
  private final val out:Outlet[CopyReport[VSBackupEntry]] = Outlet.create("VSCloseFile.out")

  override def shape: FlowShape[CopyReport[VSBackupEntry], CopyReport[VSBackupEntry]] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    def setCloseWithRetry(fileId:String, attempt:Int=0):Future[Either[HttpError,String]] =
      comm.request(VSCommunicator.OperationType.PUT,s"/API/storage/$storageId/file/$fileId/state/CLOSED",None,Map("Accept"->"application/xml")).flatMap({
        case errorResponse@ Left(err)=>
          if(err.errorCode==503 || err.errorCode==504){
            logger.warn(s"Vidispine timed out on attempt $attempt. Retrying in 2s...")
            Thread.sleep(2000)
            setCloseWithRetry(fileId,attempt+1)
          } else {
            Future(errorResponse)
          }
        case result@Right(_)=>Future(result)
      })

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)
        val maybeTargetFileId = elem.extraData.flatMap(_.newlyCreatedReplicaId)

        val completionCb = createAsyncCallback[CopyReport[VSBackupEntry]](entry=>push(out,entry))
        val failedCb = createAsyncCallback[Throwable](err=>fail(out, err))

        if(maybeTargetFileId.isEmpty){
          logger.warn(s"Incoming element $elem has no newly created file ID so can't close")
          push(out, elem)
        } else {
          setCloseWithRetry(maybeTargetFileId.get).onComplete({
            case Failure(err)=>
              logger.error("setCloseWithRetry crashed: ", err)
              failedCb.invoke(err)
            case Success(Left(err))=>
              logger.error(s"Could not request file close in Vidispine: $err")
              failedCb.invoke(new RuntimeException(err.toString))
            case Success(Right(_))=>
              logger.info(s"File ${maybeTargetFileId.get} closed")
              completionCb.invoke(elem)
          })
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

package vsStreamComponents

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.{ArchiveTargetStatus, PotentialArchiveTarget}
import org.slf4j.LoggerFactory
import vidispine.VSCommunicator
import vidispine.VSCommunicator.OperationType

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class VSDeleteShapeAndOrFile(implicit vsCommunicator:VSCommunicator, mat:Materializer) extends GraphStage[FlowShape[PotentialArchiveTarget, PotentialArchiveTarget]] {
  private final val in:Inlet[PotentialArchiveTarget] = Inlet.create("VSDeleteShapeAndOrFile.in")
  private final val out:Outlet[PotentialArchiveTarget] = Outlet.create("VSDeleteShapeAndOrFile.out")

  override def shape: FlowShape[PotentialArchiveTarget, PotentialArchiveTarget] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    private val successCb = createAsyncCallback[PotentialArchiveTarget](elem=>{
      push(out, elem.copy(status = ArchiveTargetStatus.SUCCESS))
      canTerminate=true
      if(mustTerminate) {
        completeStage()
      }
    })

    private val errorCb = createAsyncCallback[Throwable](err=>{
      failStage(err)
      canTerminate=true
      if(mustTerminate) {
        completeStage()
      }
    })

    private var canTerminate = true
    private var mustTerminate = false

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val shapeDeletionFuture = if(elem.vsItemAttachment.isDefined) {
          canTerminate=false
          val deletionFutures = elem.vsShapeAttachment.map(_.map(shapeId=>
            vsCommunicator.request(OperationType.DELETE, s"/API/item/${elem.vsItemAttachment.get}/shape/$shapeId",None,Map())
          ))
          Future.sequence(deletionFutures.getOrElse(Seq()))
        } else {
          logger.info(s"File ${elem.vsFileId} does not have an item attachment")
          Future(Seq(Right("")))
        }

        shapeDeletionFuture.onComplete({
          case Failure(err)=>
            logger.error("Deletion of shape(s) failed: ", err)
            errorCb.invoke(err)
          case Success(results)=>
            val errorSeq = results.collect({case Left(err)=>err})
            if(errorSeq.nonEmpty){
              logger.error(s"${errorSeq.length} / ${results.length} deletions failed: ")
              errorSeq.foreach(err=>logger.error(s"\t$err"))
              errorCb.invoke(new RuntimeException(s"${errorSeq.length} / ${results.length} deletions failed"))
            } else {
              logger.info(s"Deleted ${results.length} shapes")
              vsCommunicator.request(OperationType.DELETE, s"/API/file/${elem.vsFileId}",None,Map()).onComplete({
                case Failure(err)=>
                  logger.error(s"File deletion failed: ",err)
                  errorCb.invoke(err)
                case Success(Left(httpErr))=>
                  logger.error(s"File deletion failed: $httpErr")
                  errorCb.invoke(new RuntimeException("File deletion failed, see logs for detailed"))
                case Success(Right(_))=>
                  logger.info(s"File deletion of ${elem.vsFileId} completed")
                  successCb.invoke(elem)
              })
            }
        })
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.BackupEntry
import org.slf4j.LoggerFactory

/**
  * Removes the "GNM_BEING_WRITTEN" flag from OM metadata in the given BackupEntry
  */
class ClearBeingWritten extends GraphStage[FlowShape[BackupEntry, BackupEntry]] {
  private final val in:Inlet[BackupEntry] = Inlet.create("ClearBeingWritten.in")
  private final val out:Outlet[BackupEntry] = Outlet.create("ClearBeingWritten.out")

  override def shape: FlowShape[BackupEntry, BackupEntry] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val maybeCurrentMeta = elem.maybeObjectMatrixEntry.flatMap(_.attributes)
        val maybeUpdatedMeta = maybeCurrentMeta.map(_.withoutValue("GNM_BEING_WRITTEN"))

        val updatedElem = if(elem.maybeObjectMatrixEntry.isDefined){
          elem.copy(maybeObjectMatrixEntry = Some(elem.maybeObjectMatrixEntry.get.copy(attributes = maybeUpdatedMeta)))
        } else {
          elem
        }

        push(out, updatedElem)
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.{BackupEntry, BackupStatus}

/**
  * sets the Status field in the given BackupEntry to the provided value
  * @param toStatus
  */
class SetBackupStatus(toStatus: BackupStatus.Value) extends GraphStage[FlowShape[BackupEntry, BackupEntry]] {
  private final val in:Inlet[BackupEntry] = Inlet.create("SetBackupStatus.in")
  private final val out:Outlet[BackupEntry] = Outlet.create("SetBackupStatus.out")

  override def shape = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val updated = elem.copy(status = toStatus)
        push(out, updated)
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

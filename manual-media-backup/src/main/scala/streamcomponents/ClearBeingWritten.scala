package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import models.BackupEntry
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Removes the "GNM_BEING_WRITTEN" flag from OM metadata in the given BackupEntry
  */
class ClearBeingWritten(userInfo:UserInfo) extends GraphStage[FlowShape[BackupEntry, BackupEntry]] {
  private final val in:Inlet[BackupEntry] = Inlet.create("ClearBeingWritten.in")
  private final val out:Outlet[BackupEntry] = Outlet.create("ClearBeingWritten.out")

  override def shape: FlowShape[BackupEntry, BackupEntry] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    private var maybeVault:Option[Vault] = None

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val maybeCurrentMeta = elem.maybeObjectMatrixEntry.flatMap(_.attributes)
        val maybeUpdatedMeta = maybeCurrentMeta.map(_.withValue("GNM_BEING_WRITTEN",false))

        val updatedElem = elem.copy(maybeObjectMatrixEntry = Some(elem.maybeObjectMatrixEntry.get.copy(attributes = maybeUpdatedMeta)))
        push(out, updatedElem)
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })

    override def preStart(): Unit = {
      try {
        maybeVault = Some(MatrixStore.openVault(userInfo))
      } catch {
        case err:Throwable=>
          logger.error("Could not connect to vault: ", err)
          failStage(err)
      }
    }

    override def postStop(): Unit = {
      maybeVault.map(_.dispose())
    }
  }
}

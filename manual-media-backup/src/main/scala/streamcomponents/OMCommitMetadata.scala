package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import models.BackupEntry
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

class OMCommitMetadata(userInfo:UserInfo) extends GraphStage[FlowShape[BackupEntry, BackupEntry]] {
  private val in:Inlet[BackupEntry] = Inlet.create("OMCommitMetadata.in")
  private val out:Outlet[BackupEntry] = Outlet.create("OMCommitMetadata.out")

  override def shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    private var maybeVault:Option[Vault] = None

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        elem.maybeObjectMatrixEntry match {
          case Some(omEntry)=>
            omEntry.attributes match {
              case Some(attribs)=>
                try {
                  val fileToWrite = maybeVault.get.getObject(omEntry.oid)
                  val attribView = fileToWrite.getAttributeView
                  attribView.writeAllAttributes(attribs.toAttributes.asJava)
                }
            }
        }
      }
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
  }
}

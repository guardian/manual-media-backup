package streamcomponents

import java.io.File

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import helpers.{Copier, MatrixStoreHelper}
import models.{BackupEntry, ObjectMatrixEntry}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

class CreateOMFileNoCopy(userInfo:UserInfo) extends GraphStage[FlowShape[BackupEntry, BackupEntry]]{
  private final val in:Inlet[BackupEntry] = Inlet("CreateOMFileNoCopy.in")
  private final val out:Outlet[BackupEntry] = Outlet("CreateOMFileNoCopy.out")

  override def shape: FlowShape[BackupEntry, BackupEntry] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    private var maybeVault:Option[Vault] = None

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        implicit val implVault = maybeVault.get
        elem.maybeObjectMatrixEntry match {
          case Some(omEntry)=>
            logger.warning(s"CreateOMFile received an entry that already had an OM file, this indicates a code bug")
            push(out, elem)
          case None=> //we expect not to have an ObjectMatrixEntry yet
            val srcFile = elem.originalPath.toFile
            MatrixStoreHelper.metadataFromFilesystem(srcFile).flatMap(metadata=>{
                val updatedMeta = metadata
                  .withValue("GNM_BEING_WRITTEN", true)
                  .withString("MXFS_PATH",srcFile.getAbsolutePath)
                  .withString("MXFS_FILENAME", srcFile.getName)
                  .withString("MXFS_FILENAME_UPPER", srcFile.getName.toUpperCase)
                Copier.createObjectWithMetadata(Some(srcFile.getAbsolutePath),srcFile,updatedMeta)
            }) match {
              case Failure(err)=>
                logger.error("Could not create object: ", err)
                failStage(err)
              case Success((mxsObject,metadata))=>
                logger.info(s"Created new object")
                val entry = ObjectMatrixEntry(mxsObject.getId,Some(metadata),None)
                val updated = elem.copy(maybeObjectMatrixEntry = Some(entry))
                push(out, updated)
            }
        }
      }
    })

    override def preStart(): Unit = {
      maybeVault = Some(MatrixStore.openVault(userInfo))
    }

    override def postStop(): Unit = {
      maybeVault.map(_.dispose())
    }
  }
}

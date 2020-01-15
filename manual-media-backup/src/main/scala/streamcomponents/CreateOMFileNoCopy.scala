package streamcomponents

import java.io.File

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import helpers.{Copier, MatrixStoreHelper}
import models.{BackupEntry, MxsMetadata, ObjectMatrixEntry}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

class CreateOMFileNoCopy(userInfo:UserInfo) extends GraphStage[FlowShape[BackupEntry, BackupEntry]]{
  private final val in:Inlet[BackupEntry] = Inlet("CreateOMFileNoCopy.in")
  private final val out:Outlet[BackupEntry] = Outlet("CreateOMFileNoCopy.out")

  override def shape: FlowShape[BackupEntry, BackupEntry] = FlowShape.of(in,out)

  //extracts out calls to static objects to make testing easier
  def callOpenVault = Some(MatrixStore.openVault(userInfo))
  def callCreateObjectWithMetadata(filePath:Option[String], srcFile:File, meta:MxsMetadata)(implicit v:Vault) =
    Copier.createObjectWithMetadata(filePath, srcFile, meta)
  def callMetadataFromFilesystem(srcFile:File) = MatrixStoreHelper.metadataFromFilesystem(srcFile)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    private var maybeVault:Option[Vault] = None

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        implicit val vault = maybeVault.get
        elem.maybeObjectMatrixEntry match {
          case Some(omEntry)=>
            logger.warn(s"CreateOMFile received an entry that already had an OM file, this indicates a code bug")
            push(out, elem)
          case None=> //we expect not to have an ObjectMatrixEntry yet
            val srcFile = elem.originalPath.toFile
            callMetadataFromFilesystem(srcFile).flatMap(metadata=>{
                val updatedMeta = metadata
                  .withValue("GNM_BEING_WRITTEN", true)
                  .withString("MXFS_PATH",srcFile.getAbsolutePath)
                  .withString("MXFS_FILENAME", srcFile.getName)
                  .withString("MXFS_FILENAME_UPPER", srcFile.getName.toUpperCase)
                callCreateObjectWithMetadata(Some(srcFile.getAbsolutePath),srcFile,updatedMeta)
            }) match {
              case Failure(err)=>
                err match {
                  case fileio:java.nio.file.NoSuchFileException=>
                    logger.warn(s"File ${elem.originalPath} appears to not exist any more")
                    pull(in)
                  case _=>
                    logger.error("Could not create object: ", err)
                    failStage(err)
                }
              case Success((mxsObject,metadata))=>
                logger.info(s"Created new object")
                val entry = ObjectMatrixEntry(mxsObject.getId,Some(metadata),None)
                val updated = elem.copy(maybeObjectMatrixEntry = Some(entry))
                push(out, updated)
            }
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })

    override def preStart(): Unit = {
      maybeVault = callOpenVault
    }

    override def postStop(): Unit = {
      maybeVault.map(_.dispose())
    }
  }
}

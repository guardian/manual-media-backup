package streamcomponents

import java.nio.file.Path

import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FanOutShape2, Inlet, Outlet, UniformFanOutShape}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import helpers.MatrixStoreHelper
import models.{BackupEntry, ObjectMatrixEntry}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
  * checks if the incoming Path points to something that exists in the Vault pointed to by the provided UserInfo.
  * if it does, then it's pushed to the "Yes" port alongside the ObjectMatrixEntry (unpopulated);
  * if it doesn't then it's pushed to the "No" port.
  * @param userInfo UserInfo object identifying the appliance and vault to check. This is logged in to when the stream starts
  *                 up and logged out of when the stream completes. Each instance of the stream will make its own login.
  */
class CheckOMFile(userInfo:UserInfo) extends GraphStage[UniformFanOutShape[BackupEntry,BackupEntry]] {
  private final val in:Inlet[BackupEntry] = Inlet.create("CheckOMFile.in")
  private final val yes:Outlet[BackupEntry] = Outlet.create("CheckOMFile.yes")
  private final val no:Outlet[BackupEntry] = Outlet.create("CheckOMFile.no")

  override def shape: UniformFanOutShape[BackupEntry,BackupEntry] = new UniformFanOutShape[BackupEntry,BackupEntry](in, Array(yes,no))

  def callOpenVault = Some(MatrixStore.openVault(userInfo))
  /**
    * helper method to make testing easier
    * @param vault
    * @param path
    * @return
    */
  def callFindByFilename(vault:Vault, path:String) = MatrixStoreHelper.findByFilename(vault,path)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    private var vault:Option[Vault] = None

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        callFindByFilename(vault.get, elem.originalPath.toString) match {
          case Success(results)=>
            if(results.isEmpty){
              logger.info(s"Path ${elem.toString} does not exist in vault")
              push(no, elem)
            } else {
              logger.debug(s"Path ${elem.toString} does exist in the vault")
              if(results.length>1) logger.warn(s"Got ${results.length} entries for ${elem.toString}, using the first")
              val updated = results.head.getMetadataSync(vault.get)
              val finalOutput = elem.copy(maybeObjectMatrixEntry = Some(updated))
              push(yes,finalOutput)
            }
          case Failure(err)=>
            logger.error(s"Could not look up ${elem.originalPath.toString} on $vault: ", err)
            failStage(err)
        }
      }
    })

    setHandler(yes, new AbstractOutHandler {
      override def onPull(): Unit = if(!hasBeenPulled(in)) pull(in)
    })

    setHandler(no, new AbstractOutHandler {
      override def onPull(): Unit = if(!hasBeenPulled(in)) pull(in)
    })

    override def preStart(): Unit = {
      vault = callOpenVault
    }

    override def postStop(): Unit = {
      logger.info("Tearing down CheckOMFile")
      vault.map(_.dispose())
    }
  }
}

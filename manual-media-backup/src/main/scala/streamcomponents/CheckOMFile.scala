package streamcomponents

import java.nio.file.Path

import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FanOutShape2, Inlet, Outlet, UniformFanOutShape}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import helpers.MatrixStoreHelper
import models.{BackupEntry, ObjectMatrixEntry}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

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

        logger.debug(s"Looking for ${elem.originalPath.toString}...")
        val metadataUpdate = callFindByFilename(vault.get, elem.originalPath.toString)
          .flatMap(results=> {
            val trySeq = results.map(result => Try {
              result.getMetadataSync(vault.get)
            })
            val failures = trySeq.collect({case Failure(err)=>err})
            if(failures.nonEmpty){
              Failure(failures.head)
            } else {
              Success(trySeq.collect({case Success(m)=>m}))
            }
          })


        metadataUpdate match {
          case Success(results)=>
            logger.debug(s"Got ${results.length} results including ${results.headOption}")
            if(results.length>1){
              logger.warn(s"Got ${results.length} results for ${elem.originalPath}, using the first")
            }
            if(results.isEmpty) {
              push(no, elem)
            } else {
              val updatedElem = elem.copy(maybeObjectMatrixEntry = Some(results.head))
              push(yes, updatedElem)
            }
          case Failure(err:java.io.IOException)=>
            if(err.getMessage.contains("error 311")){
              logger.warn(s"Ignoring ObjectMatrix can't lock file error for ${elem.originalPath.toString}, moving along to next file")
              pull(in)
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

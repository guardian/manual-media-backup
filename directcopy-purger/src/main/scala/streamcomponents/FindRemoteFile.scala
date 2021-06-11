package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import helpers.{MatrixStoreHelper, MetadataHelper}
import models.{FileEntry, PathTransformSet, RemoteFile}
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import scala.util.{Failure, Success, Try}

class FindRemoteFile(mxs:MatrixStore, vaultId:String, maybePathTransformSet: Option[PathTransformSet]=None) extends GraphStage[FlowShape[FileEntry, FileEntry]] {
  private final val logger = LoggerFactory.getLogger(getClass)
  private final val in:Inlet[FileEntry] = Inlet.create("FindRemoteFile.in")
  private final val out:Outlet[FileEntry] = Outlet.create("FindRemoteFile.out")

  override def shape: FlowShape[FileEntry, FileEntry] = FlowShape.of(in, out)

  private def internalGetMd5(vault:Vault, oid:String) = Try {
    val obj = vault.getObject(oid)
    val v = obj.getAttributeView
    val buf = ByteBuffer.allocate(16)
    v.read("__mxs__calc_md5", buf)
    Hex.encodeHexString(buf)
  }.flatMap(hexString=>{
    if(hexString.length!=32)
      Failure(new RuntimeException(s"final hex string was ${hexString.length} bytes long and should be 32"))
    else
      Success(hexString)
  })

  def getMd5(vault:Vault, oid:String, attempt:Int, maxAttempts:Int):Try[String] = {
      internalGetMd5(vault, oid) match {
        case result@Success(_)=>
          result
        case problem@Failure(err)=>
          logger.error(s"Could not get checksum on attempt $attempt: ${err.getMessage}")
          if(attempt<maxAttempts) {
            Thread.sleep(2000)
            getMd5(vault, oid, attempt+1, maxAttempts)
          } else {
            logger.error(s"Could not get checksum after $maxAttempts attempts, giving up")
            problem
          }
      }
  }

  def callFindByFilename(vault: Vault, filepath: String) = MatrixStoreHelper.findByFilename(vault,filepath, Seq("oid","__mxs__length"))

  def callOpenVault() = mxs.openVault(vaultId)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var vault:Option[Vault] = _
    private var counter:Int = 0

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val maybeTransformedPath = for {
          pathTransform <- maybePathTransformSet
          newPath <- pathTransform.apply(elem.localFile.filePath).toOption.flatten
        } yield newPath

        val pathToFind = maybeTransformedPath match {
          case None=>elem.localFile.filePath.toString
          case Some(transformed)=>transformed.toString
        }

        logger.debug(s"Finding file ${counter+1}")
        callFindByFilename(vault.get, pathToFind) match {
          case Success(foundFiles)=>
            if(foundFiles.isEmpty) {
              logger.warn(s"Could not find anything for ${elem.localFile.filePath.toString}")
              counter+=1
              push(out, elem)
            } else if(foundFiles.length>1) {
              logger.warn(s"Found ${foundFiles.length} files matching ${elem.localFile.filePath.toString}")
              failStage(new RuntimeException("Multiple files found"))
            } else {
              logger.debug(s"Getting md5 for ${foundFiles.head.oid} ($pathToFind)")
              getMd5(vault.get, foundFiles.head.oid, 0, 20) match {
                case Success(md5)=>
                  val updatedElem = elem.copy(remoteFile = Some(
                    RemoteFile(foundFiles.head.oid,
                               foundFiles.head.getFileSize.getOrElse(-1L),
                                Some(md5)
                    )
                  ))
                  counter+=1
                  push(out, updatedElem)
                case Failure(err)=>
                  logger.error(s"Could not look up file: ${err.getMessage}")
                  failStage(err)
              }
            }
        }
      }
    })

    override def preStart(): Unit = {
      try {
        vault = Some(callOpenVault())
      } catch {
        case err:Throwable=>
          logger.error(s"Could not connect to the vault: ${err.getMessage}", err)
          throw err
      }
    }

    override def postStop(): Unit = {
      vault.map(_.dispose())
    }
  }
}

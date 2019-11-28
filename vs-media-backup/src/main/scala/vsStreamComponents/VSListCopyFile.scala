package vsStreamComponents

import java.io.File

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, AsyncCallback, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{UserInfo, Vault}
import helpers.Copier
import models.{CopyReport, IncomingListEntry, VSBackupEntry}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * perform a single file copy in a bulk file copy operation.  This will spin up an entire substream to perform
  * the file copy.
  * @param vault
  * @param chunkSize
  * @param mat
  */
class VSListCopyFile(userInfo:UserInfo, vault:Vault,chunkSize:Int)(implicit val mat:Materializer)
  extends GraphStage[FlowShape[VSBackupEntry,CopyReport[VSBackupEntry]]] {
  private final val in:Inlet[VSBackupEntry] = Inlet.create("VSListCopyFile.in")
  private final val out:Outlet[CopyReport[VSBackupEntry]] = Outlet.create("VSListCopyFile.out")

  override def shape: FlowShape[VSBackupEntry, CopyReport[VSBackupEntry]] = FlowShape.of(in,out)

  val checksumType = "md5"  //turn off checksum due to issues

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        logger.debug(s"listCopyFile: onPush")
        val entry = grab(in)

        if(entry.fullPath.isEmpty){
          logger.error("Item has no full path, can't continue.")
          pull(in)
        } else {
          val fullPath = entry.fullPath.get
          val f = new File(fullPath)
          val fileSize = f.length()

          logger.info(s"Starting copy of $fullPath")
          val completedCb = createAsyncCallback[CopyReport[VSBackupEntry]](report => push(out, report))
          val failedCb = createAsyncCallback[Throwable](err => failStage(err))

          Copier.copyFromLocal(userInfo, vault, entry.storageSubpath, fullPath, chunkSize, checksumType).onComplete({
            case Success(Right((oid, maybeChecksum))) =>
              logger.info(s"Copied $fullPath to $oid")
              completedCb.invoke(CopyReport(fullPath, oid, maybeChecksum, fileSize, preExisting = false, validationPassed = None, extraData = Some(entry)))
            case Success(Left(copyProblem)) =>
              logger.warn(s"Could not copy file: $copyProblem")
              completedCb.invoke(CopyReport(fullPath, copyProblem.filepath.oid, None, fileSize, preExisting = true, validationPassed = None, extraData = Some(entry)))
            case Failure(err) =>
              logger.info(s"Failed copying $fullPath", err)
              failedCb.invoke(err)
          })
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = {
        logger.debug("listCopyFile: onPull")
        pull(in)
      }
    })
  }
}

package streamcomponents

import java.io.File

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, AsyncCallback, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{UserInfo, Vault}
import helpers.Copier
import models.{CopyReport, IncomingListEntry, ObjectMatrixEntry}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import org.apache.commons.io.FilenameUtils

/**
  * perform a single file copy in a bulk file copy operation.  This will spin up an entire substream to perform
  * the file copy.
  * @param vault
  * @param chunkSize
  * @param checksumType
  * @param mat
  */
class ListRestoreFile[T](userInfo:UserInfo, vault:Vault,chunkSize:Int, checksumType:String, toPath:Option[String], implicit val mat:Materializer)
  extends GraphStage[FlowShape[ObjectMatrixEntry,CopyReport[T]]] {
  private final val in:Inlet[ObjectMatrixEntry] = Inlet.create("ListRestoreFile.in")
  private final val out:Outlet[CopyReport[T]] = Outlet.create("ListRestoreFile.out")

  override def shape: FlowShape[ObjectMatrixEntry, CopyReport[T]] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        logger.debug(s"ListRestoreFile: onPush")
        val entry = grab(in)

        logger.info(s"Starting copy of ${entry.oid}")
        val completedCb = createAsyncCallback[CopyReport[T]](report=>push(out, report))
        val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

        val maybeOutPath = toPath.flatMap(p=>entry.maybeGetFilename().map(fn=>FilenameUtils.concat(p,fn)))

        maybeOutPath match {
          case Some(outPath)=>
            Copier.copyFromRemote(userInfo, vault, Some(outPath), entry, chunkSize, checksumType).onComplete({
              case Success(Right( (filePath,maybeChecksum) ))=>
                logger.info(s"Copied ${entry.oid} to $outPath")
                completedCb.invoke(CopyReport[T](filePath, entry.oid, maybeChecksum, entry.longAttribute("DPSP_SIZE").getOrElse(-1), preExisting = false, validationPassed = None))
              case Success(Left(copyProblem))=>
                logger.warn(s"Could not copy file: $copyProblem")
                completedCb.invoke(CopyReport[T](copyProblem.filepath.oid, "", None, entry.longAttribute("DPSP_SIZE").getOrElse(-1), preExisting = true, validationPassed = None))
              case Failure(err)=>
                logger.info(s"Failed copying $entry", err)
                failedCb.invoke(err)
            })
          case None=>
            logger.warn(s"Copying ${entry.oid}: No filename to output the file to ")
            failedCb.invoke(new RuntimeException("No filename to output"))
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

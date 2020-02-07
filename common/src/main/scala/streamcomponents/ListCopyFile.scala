package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, AsyncCallback, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import helpers.Copier
import models.{CopyReport, IncomingListEntry}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * perform a single file copy in a bulk file copy operation.  This will spin up an entire substream to perform
  * the file copy.
  * @param chunkSize
  * @param checksumType
  * @param mat
  */
class ListCopyFile[T](userInfo:UserInfo, chunkSize:Int, checksumType:String, implicit val mat:Materializer)
  extends GraphStage[FlowShape[IncomingListEntry,CopyReport[T]]] {
  private final val in:Inlet[IncomingListEntry] = Inlet.create("ListCopyFile.in")
  private final val out:Outlet[CopyReport[T]] = Outlet.create("ListCopyFile.out")

  override def shape: FlowShape[IncomingListEntry, CopyReport[T]] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    private var maybeVault:Option[Vault] = None

    val completedCb = createAsyncCallback[CopyReport[T]](report=>push(out, report))
    val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

    private var canComplete = true
    private var upstreamCompleted = false

    setHandler(in, new AbstractInHandler {
      override def onUpstreamFinish(): Unit = {
        if(canComplete){
          completeStage()
        } else {
          logger.info("Upstream completed but we are not ready yet")
          upstreamCompleted=true
        }
      }

      override def onPush(): Unit = {
        logger.debug(s"listCopyFile: onPush")
        val entry = grab(in)

        if(maybeVault.isEmpty){
          failStage(new RuntimeException("ListCopyFile running without a vault, this should not happen"))
          return
        }

        logger.info(s"Starting copy of ${entry.filepath}")
        canComplete = false

        Copier.copyFromLocal(userInfo, maybeVault.get, Some(entry.filePath), entry.filepath, chunkSize, checksumType).onComplete({
          case Success(Right( (oid,maybeChecksum) ))=>
            logger.info(s"Copied ${entry.filepath} to $oid")
            completedCb.invoke(CopyReport[T](entry.filePath, oid, maybeChecksum, entry.size, preExisting = false, validationPassed = None))
            canComplete=true
            if(upstreamCompleted) completeStage()
          case Success(Left(copyProblem))=>
            logger.warn(s"Could not copy file: $copyProblem")
            completedCb.invoke(CopyReport[T](entry.filePath, copyProblem.filepath.oid, None, entry.size, preExisting = true, validationPassed = None))
            canComplete=true
            if(upstreamCompleted) completeStage()
          case Failure(err)=>
            logger.info(s"Failed copying ${entry.filepath}", err)
            failedCb.invoke(err)
            canComplete=true
            if(upstreamCompleted) completeStage()
        })
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = {
        logger.debug("listCopyFile: onPull")
        pull(in)
      }
    })

    override def preStart(): Unit = {
      try {
        maybeVault = Some(MatrixStore.openVault(userInfo))
      } catch {
        case err:Throwable=>
          logger.error(s"ListCopyFile could not connect to the requested vault: ", err)
          failStage(err)
      }
    }

    override def postStop(): Unit = {
      maybeVault.map(_.dispose())
    }
  }
}

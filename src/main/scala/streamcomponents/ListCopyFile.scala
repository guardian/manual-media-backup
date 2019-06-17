package streamcomponents

import java.io.File

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AsyncCallback, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{UserInfo, Vault}
import helpers.Copier
import models.{CopyReport, IncomingListEntry}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * perform a single file copy in a bulk file copy operation.  This will spin up an entire substream to perform
  * the file copy.
  * @param vault
  * @param chunkSize
  * @param checksumType
  * @param mat
  */
class ListCopyFile(userInfo:UserInfo, vault:Vault,chunkSize:Int, checksumType:String, implicit val mat:Materializer)
  extends GraphStage[FlowShape[IncomingListEntry,CopyReport]] {
  private final val in:Inlet[IncomingListEntry] = Inlet.create("ListCopyFile.in")
  private final val out:Outlet[CopyReport] = Outlet.create("ListCopyFile.out")

  override def shape: FlowShape[IncomingListEntry, CopyReport] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val entry = grab(in)

        val completedCb = createAsyncCallback[CopyReport](report=>push(out, report))
        val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

        Copier.copyFromLocal(userInfo, vault, Some(entry.filePath), entry.filePath, chunkSize, checksumType).onComplete({
          case Success((oid,checksum))=>
            completedCb.invoke(CopyReport(entry.filePath, oid,checksum, entry.size))
          case Failure(err)=>
            failedCb.invoke(err)
        })
      }
    })
  }
}

package streamcomponents

import java.io.File

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AsyncCallback, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{UserInfo, Vault}
import helpers.Copier

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
class ListCopyFile(userInfo:UserInfo, vault:Vault,chunkSize:Int, checksumType:String, implicit val mat:Materializer) extends GraphStage[FlowShape[String,String]] {
  private final val in:Inlet[String] = Inlet.create("ListCopyFile.in")
  private final val out:Outlet[String] = Outlet.create("ListCopyFile.out")

  override def shape: FlowShape[String, String] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val filename = grab(in)

        val completedCb = createAsyncCallback[String](checksum=>push(out, checksum))
        val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

        Copier.copyFromLocal(userInfo, vault, Some(filename), filename, chunkSize, checksumType).onComplete({
          case Success(result)=>completedCb.invoke(result)
          case Failure(err)=>failedCb.invoke(err)
        })
      }
    })
  }
}

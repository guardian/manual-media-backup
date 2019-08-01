import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.Vault
import helpers.MatrixStoreHelper
import models.ObjectMatrixEntry
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class ShowMxsChecksum(vault:Vault) extends GraphStage[FlowShape[ObjectMatrixEntry,ObjectMatrixEntry]] {
  private val in:Inlet[ObjectMatrixEntry] = Inlet.create("ShowMxsChecksum.in")
  private val out:Outlet[ObjectMatrixEntry] = Outlet.create("ShowMxsChecksum.out")

  override def shape: FlowShape[ObjectMatrixEntry, ObjectMatrixEntry] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val mxsObject = vault.getObject(elem.oid)

        val completionCb = createAsyncCallback[ObjectMatrixEntry](el=>push(out,el))
        val failureCb = createAsyncCallback[Throwable](err=>failStage(err))

        MatrixStoreHelper.getOMFileMd5(mxsObject).onComplete({
          case Failure(err)=>
            logger.error(s"getOMFileMD5 for ${elem} crashed: ", err)
            failureCb.invoke(err)
          case Success(Failure(err))=>
            logger.error(s"Could not get MD5 for ${elem}: ", err)
            completionCb.invoke(elem)
          case Success(Success(md5String))=>
            logger.info(s"Got MD5 $md5String for ${elem.pathOrFilename}")
            completionCb.invoke(elem)
        })
      }
    })
  }
}

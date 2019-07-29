package vsStreamComponents

import java.net.URLEncoder

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.VSBackupEntry
import org.slf4j.LoggerFactory
import vidispine.{VSCommunicator, VSFile}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class CreateFileDuplicate (comm:VSCommunicator, toStorageId:String)(implicit mat:Materializer) extends GraphStage[FlowShape[VSBackupEntry,VSBackupEntry]] {
  private val in:Inlet[VSBackupEntry] = Inlet.create("CreateFileDuplicate.in")
  private val out:Outlet[VSBackupEntry] = Outlet.create("CreateFileDuplicate.out")

  override def shape: FlowShape[VSBackupEntry, VSBackupEntry] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val completedCb = createAsyncCallback[VSBackupEntry](e=>push(out,e))
        val failedCb = createAsyncCallback[Throwable](err=>fail(out, err))

        val encodedPath = elem.storageSubpath.map(URLEncoder.encode(_,"UTF-8"))
        val pathParam = encodedPath.map(p=>s"&path=$p").getOrElse("")

        val uri = s"/API/storage/${elem.sourceStorage.get}/file/${elem.vsFileId.get}/path"
        val queryParams = Map(
          "storage"->toStorageId,
          "duplicate"->"true"
        ) ++ elem.storageSubpath.map(p=>Map("path"->p)).getOrElse(Map())

        comm.request(VSCommunicator.OperationType.POST,uri,None,Map(),queryParams = queryParams).onComplete({
          case Failure(err)=>
            logger.error("VS request crashed:", err)
            failedCb.invoke(err)
          case Success(Left(httpError))=>
            logger.error(s"VS returned an error: $httpError")
            httpError.errorCode match {
              case 404=>pull(in)  //Not Found, so we can continue
              case _=>failedCb.invoke(new RuntimeException(httpError.toString))
            }

          case Success(Right(responseString))=>
            logger.info(s"Successfully created duplicate for ${elem.vsFileId} on ${elem.sourceStorage}: $responseString")
            VSFile.fromXmlString(responseString) match {
              case Success(Some(vsFile))=>
                val updatedOutput = elem.copy(newlyCreatedReplicaId = Some(vsFile.vsid))
                completedCb.invoke(updatedOutput)
              case Success(None)=>
                logger.error(s"Copy operation returned success but could not identify file in result")
                failedCb.invoke(new RuntimeException("Could not find copied file"))
              case Failure(err)=>
                logger.error(s"Copy operation returned success but could not parse returned XML: ", err)
                failedCb.invoke(err)
            }
        })
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

package streamComponents

import akka.http.scaladsl.model.ContentType
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Keep
import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic}
import com.gu.vidispineakka.vidispine.{VSCommunicator, VSLazyItem}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class UploadItemThumbnail(bucketName:String, cannedAcl:CannedAcl) (implicit comm:VSCommunicator, mat:Materializer)
  extends GraphStage[FlowShape[VSLazyItem, VSLazyItem]] with FilenameHelpers {

  private val in:Inlet[VSLazyItem] = Inlet.create("UploadItemThumbnail.in")
  private val out:Outlet[VSLazyItem] = Outlet.create("UploadItemThumbnail.out")

  override def shape: FlowShape[VSLazyItem, VSLazyItem] = FlowShape.of(in, out)

  //included here to make testing easier
  def getUploadSink(outputFilename:String) = S3.multipartUpload(bucketName,outputFilename, contentType=ContentType.parse("image/jpeg").right.get, cannedAcl=cannedAcl)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    val completedCb = createAsyncCallback[VSLazyItem](i=>push(out,i))
    val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val maybeSourceFuture = elem.getSingle("representativeThumbnail") match {
          case Some(tnUrl)=>
            comm
              .sendGeneric(VSCommunicator.OperationType.GET, tnUrl,None,Map(),Map())
              .map(_.body)
            .map({
              case Left(err)=>throw new RuntimeException(s"Could not get thumbnail content: $err")
              case Right(src)=>src
            })
          case None=>
            Future.failed(new NoThumbnailErr())
        }

        val outputFilename = determineFileName(elem, None) match {
          case Some(fn)=>fn
          case None=>elem.itemId + ".jpg"
        }

        val resultFuture = maybeSourceFuture.flatMap(src=>{
          val sink = getUploadSink(outputFilename)
          src.toMat(sink)(Keep.right).run()
        })

        resultFuture.onComplete({
          case Success(uploadResult)=>
            logger.info(s"Completed uploading thumbnail for ${elem.itemId} to ${uploadResult.location}")
            completedCb.invoke(elem)
          case Failure(err:NoThumbnailErr)=>
            logger.warn(s"The item ${elem.itemId} had no thumbnail attached")
            completedCb.invoke(elem)
          case Failure(other)=>
            logger.error(s"Could not upload thumbnail for ${elem.itemId}: ", other)
            failedCb.invoke(other)
        })
      }
    })
  }

  class NoThumbnailErr extends RuntimeException
}

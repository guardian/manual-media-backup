package streamComponents

import akka.http.scaladsl.model.ContentType
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.util.ByteString
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
  def runCopy(src:Source[ByteString,Any], sink:Sink[ByteString,Future[MultipartUploadResult]]) = src.toMat(sink)(Keep.right).run()

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    val completedCb = createAsyncCallback[VSLazyItem](i=>push(out,i))
    val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

    setHandler(in, new AbstractInHandler {
      private var canComplete:Boolean = true

      override def onPush(): Unit = {
        val elem = grab(in)
        canComplete = false

        val maybeSourceFuture = elem.getSingle("representativeThumbnail") match {
          case Some(tnUrl)=>
            comm
              .sendGeneric(VSCommunicator.OperationType.GET, tnUrl, None,headers=Map("Accept"->"*/*"), queryParams=Map())
              .map(_.body)
            .map({
              case Left(err)=>throw new RuntimeException(s"Could not get thumbnail content: $err")
              case Right(src)=>src
            })
          case None=>
            Future.failed(new NoThumbnailErr())
        }

        val maybeOriginalShape = elem.shapes.flatMap(_.get("original"))

        val outputFilename = determineFileName(elem, maybeOriginalShape) match {
          case Some(fn)=>
            val strippedFn = removeExtension(fn)
            strippedFn + "_thumb.jpg"
          case None=>elem.itemId + "_thumb.jpg"
        }

        val resultFuture = maybeSourceFuture.flatMap(src=>{
          val sink = getUploadSink(outputFilename)
          runCopy(src,sink)
        })

        resultFuture.onComplete({
          case Success(uploadResult)=>
            logger.info(s"Completed uploading thumbnail for ${elem.itemId} to ${uploadResult.location}")
            canComplete = true
            completedCb.invoke(elem)
          case Failure(err:NoThumbnailErr)=>
            canComplete = true
            logger.warn(s"The item ${elem.itemId} had no thumbnail attached")
            completedCb.invoke(elem)
          case Failure(other)=>
            canComplete = true
            logger.error(s"Could not upload thumbnail for ${elem.itemId}: ", other)
            failedCb.invoke(other)
        })
      }

      override def onUpstreamFinish(): Unit = {
        var i:Int=0
        while(!canComplete){
          logger.warn(s"Upstream completed but still waiting for async processing")
          i+=1
          if(i>10){
            logger.error("Timed out waiting for async processing")
            canComplete=true
          } else {
            Thread.sleep(1000)
          }
        }
        completeStage()
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = if(!isClosed(in)) pull(in)
    })
  }

  class NoThumbnailErr extends RuntimeException
}

package streamcomponents

import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic, GraphStageWithMaterializedValue}
import akka.util.ByteString
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Builder, AmazonS3ClientBuilder}
import models.S3Target
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import com.amazonaws.services.s3.model.{AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompleteMultipartUploadResult, InitiateMultipartUploadRequest, PartETag, UploadPartRequest}
import helpers.ByteBufferBackedInputStream

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class SyncS3Uploader(s3Target:S3Target, client:AmazonS3,bufferCapacity:Int=5*1024*1024) extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[CompleteMultipartUploadResult]] {
  private final val in:Inlet[ByteString] = Inlet.create("SyncS3Uploader.in")

  override def shape = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val completionPromise = Promise[CompleteMultipartUploadResult]

    val logic = new GraphStageLogic(shape) {
      private val logger = LoggerFactory.getLogger(getClass)

      private var buffer:ByteBuffer = _
      private var uploadId:String = _
      private var partCounter:Int = 1

      private var uploadedEtags:Seq[PartETag] = Seq()

      def pushBufferContent(isLast:Boolean, partLength:Int) = Try {
        buffer.flip()
        val s = new ByteBufferBackedInputStream(buffer)

        val rq = new UploadPartRequest()
          .withBucketName(s3Target.bucket)
          .withKey(s3Target.path)
          .withLastPart(isLast)
          .withUploadId(uploadId)
          .withPartNumber(partCounter)
          .withPartSize(partLength)
          .withInputStream(s)

        val result = client.uploadPart(rq)
        logger.info(s"uploaded chunk $partCounter of size $partLength, got ${result.getETag}")
        s.close()
        partCounter+=1
        uploadedEtags = uploadedEtags :+ result.getPartETag
        result
      }

      setHandler(in, new AbstractInHandler {
        override def onPush(): Unit = {
          val newData = grab(in).toArray
          logger.info(s"got ${newData.length} more data, remaining is ${buffer.remaining()}, limit is ${buffer.limit()}...")
          if(newData.length > buffer.remaining()) {
            pushBufferContent(false, buffer.limit() - buffer.remaining()) match {
              case Success(_)=>
                buffer.clear()
                buffer.put(newData)
                pull(in)
              case Failure(err)=>
                failStage(err)
            }
          } else {
            buffer.put(newData)
            pull(in)
          }
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          logger.error(s"Upstream failed, aborting multipart upload...")
          client.abortMultipartUpload(new AbortMultipartUploadRequest(s3Target.bucket, s3Target.path, uploadId))
          completionPromise.failure(ex)
          super.onUpstreamFailure(ex)
        }

        override def onUpstreamFinish(): Unit = {
          logger.info("Upstream completed, completing multipart upload...")
          if(buffer.limit() - buffer.remaining()>0) {
            pushBufferContent(true, buffer.limit()-buffer.remaining())
          }

          val rq = new CompleteMultipartUploadRequest()
            .withBucketName(s3Target.bucket)
            .withKey(s3Target.path)
            .withPartETags(uploadedEtags.asJava)
            .withUploadId(uploadId)
          val result = client.completeMultipartUpload(rq)
          completionPromise.success(result)
        }
      })

      override def preStart(): Unit = {
        try {
          val rq = new InitiateMultipartUploadRequest(s3Target.bucket, s3Target.path)
          val initiateResponse = client.initiateMultipartUpload(rq)
          uploadId = initiateResponse.getUploadId
          logger.info(s"upload ID is $uploadId")
          buffer = ByteBuffer.allocate(bufferCapacity)
          pull(in)
        } catch {
          case err:Throwable =>
            failStage(err)
        }
      }
    }

    (logic, completionPromise.future)
  }
}

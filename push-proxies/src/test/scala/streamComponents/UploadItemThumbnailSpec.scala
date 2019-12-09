package streamComponents

import akka.http.scaladsl.model.Uri
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer, SinkShape}
import akka.util.ByteString
import com.gu.vidispineakka.vidispine.{VSCommunicator, VSLazyItem}
import com.softwaremill.sttp.Response
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class UploadItemThumbnailSpec extends Specification with Mockito {
  "UploadItemThumbnail" should {
    "look up the URL provided by representativeThumbnail, open a stream to it and push it to the sink" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val elem = mock[VSLazyItem]
      elem.getSingle("representativeThumbnail") returns Some("https://some/kinda/url/to/thumbnail.jpg")
      elem.getSingle("gnm_asset_filename") returns Some("path/to/media.mxf")
      elem.itemId returns "VX-1234"
      val fakeContentSource = Source.single(ByteString("file content here"))
      implicit val mockedCommunicator = mock[VSCommunicator]
      val mockedResponse = mock[Response[Source[ByteString,Any]]]
      mockedResponse.body returns Right(fakeContentSource)
      mockedCommunicator.sendGeneric(any,any,any,any,any) returns Future(mockedResponse)

      val getUploadSinkArgsCheck = mock[String=>Unit]
      val runCopyArgsCheck = mock[(Source[ByteString,Any],Sink[ByteString,Future[MultipartUploadResult]])=>Unit]

      val toTest = new UploadItemThumbnail("somebucket",CannedAcl.Private) {
        /**
          * override the real upload sink with a fake one that copies the content into a convienient string field in MultipartUploadResult
          * @param outputFilename
          * @return
          */
        override def getUploadSink(outputFilename: String): Sink[ByteString, Future[MultipartUploadResult]] = {
          getUploadSinkArgsCheck(outputFilename)
          val sinkFact = Sink.reduce[MultipartUploadResult]((acc,elem)=>elem)
          Sink.fromGraph(GraphDSL.create(sinkFact) { implicit builder=> sink=>
            import akka.stream.scaladsl.GraphDSL.Implicits._

            val inputElem = builder.add(Flow.fromFunction[ByteString,MultipartUploadResult](str=>{
              MultipartUploadResult(Uri("s3://some-bucket/path/to/media_thumb.jpg"),null,null,str.utf8String,None)
            }))

            inputElem ~> sink

            SinkShape(inputElem.in)
          })
        }

        override def runCopy(src: Source[ByteString, Any], sink: Sink[ByteString, Future[MultipartUploadResult]]): Future[MultipartUploadResult] = {
          runCopyArgsCheck(src,sink)
          super.runCopy(src, sink)
        }
      }

      val resultFut = Source.single(elem)
        .via(toTest)
        .toMat(Sink.seq)(Keep.right)
        .run()

      val result = Await.result(resultFut, 30 seconds)

      there was one(getUploadSinkArgsCheck).apply("path/to/media_thumb.jpg")
      there was one(runCopyArgsCheck).apply(any,any)
    }

    "use the itemId for the filename if none could be determined" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val elem = mock[VSLazyItem]
      elem.getSingle("representativeThumbnail") returns Some("https://some/kinda/url/to/thumbnail.jpg")
      elem.getSingle("gnm_asset_filename") returns None
      elem.itemId returns "VX-1234"
      val fakeContentSource = Source.single(ByteString("file content here"))
      implicit val mockedCommunicator = mock[VSCommunicator]
      val mockedResponse = mock[Response[Source[ByteString,Any]]]
      mockedResponse.body returns Right(fakeContentSource)
      mockedCommunicator.sendGeneric(any,any,any,any,any) returns Future(mockedResponse)

      val getUploadSinkArgsCheck = mock[String=>Unit]
      val runCopyArgsCheck = mock[(Source[ByteString,Any],Sink[ByteString,Future[MultipartUploadResult]])=>Unit]

      val toTest = new UploadItemThumbnail("somebucket",CannedAcl.Private) {
        /**
          * override the real upload sink with a fake one that copies the content into a convienient string field in MultipartUploadResult
          * @param outputFilename
          * @return
          */
        override def getUploadSink(outputFilename: String): Sink[ByteString, Future[MultipartUploadResult]] = {
          getUploadSinkArgsCheck(outputFilename)
          val sinkFact = Sink.reduce[MultipartUploadResult]((acc,elem)=>elem)
          Sink.fromGraph(GraphDSL.create(sinkFact) { implicit builder=> sink=>
            import akka.stream.scaladsl.GraphDSL.Implicits._

            val inputElem = builder.add(Flow.fromFunction[ByteString,MultipartUploadResult](str=>{
              MultipartUploadResult(Uri("s3://some-bucket/path/to/media_thumb.jpg"),null,null,str.utf8String,None)
            }))

            inputElem ~> sink

            SinkShape(inputElem.in)
          })
        }

        override def runCopy(src: Source[ByteString, Any], sink: Sink[ByteString, Future[MultipartUploadResult]]): Future[MultipartUploadResult] = {
          runCopyArgsCheck(src,sink)
          super.runCopy(src, sink)
        }
      }

      val resultFut = Source.single(elem)
        .via(toTest)
        .toMat(Sink.seq)(Keep.right)
        .run()

      val result = Await.result(resultFut, 30 seconds)

      there was one(getUploadSinkArgsCheck).apply("VX-1234_thumb.jpg")
      there was one(runCopyArgsCheck).apply(any,any)
    }

    "fail if the copyGraph fails" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val elem = mock[VSLazyItem]
      elem.getSingle("representativeThumbnail") returns Some("https://some/kinda/url/to/thumbnail.jpg")
      elem.getSingle("gnm_asset_filename") returns Some("path/to/media.mxf")

      val fakeContentSource = Source.single(ByteString("file content here"))
      implicit val mockedCommunicator = mock[VSCommunicator]
      val mockedResponse = mock[Response[Source[ByteString,Any]]]
      mockedResponse.body returns Right(fakeContentSource)
      mockedCommunicator.sendGeneric(any,any,any,any,any) returns Future(mockedResponse)

      val getUploadSinkArgsCheck = mock[String=>Unit]
      val runCopyArgsCheck = mock[(Source[ByteString,Any],Sink[ByteString,Future[MultipartUploadResult]])=>Unit]

      val toTest = new UploadItemThumbnail("somebucket",CannedAcl.Private) {
        /**
          * override the real upload sink with a fake one that copies the content into a convienient string field in MultipartUploadResult
          * @param outputFilename
          * @return
          */
        override def getUploadSink(outputFilename: String): Sink[ByteString, Future[MultipartUploadResult]] = {
          getUploadSinkArgsCheck(outputFilename)
          val sinkFact = Sink.reduce[MultipartUploadResult]((acc,elem)=>elem)
          Sink.fromGraph(GraphDSL.create(sinkFact) { implicit builder=> sink=>
            import akka.stream.scaladsl.GraphDSL.Implicits._

            val inputElem = builder.add(Flow.fromFunction[ByteString,MultipartUploadResult](str=>{
              MultipartUploadResult(Uri("s3://some-bucket/path/to/media_thumb.jpg"),null,null,str.utf8String,None)
            }))

            inputElem ~> sink

            SinkShape(inputElem.in)
          })
        }

        override def runCopy(src: Source[ByteString, Any], sink: Sink[ByteString, Future[MultipartUploadResult]]): Future[MultipartUploadResult] = Future.failed(new RuntimeException("Kaboom"))
      }

      val resultFut = Source.fromIterator(()=>Seq(elem,elem).toIterator)
        .via(toTest)
        .toMat(Sink.seq)(Keep.right)
        .run()

      def theTest = {
        Await.result(resultFut, 30 seconds)
      }

      theTest must throwA[RuntimeException]
      there was one(getUploadSinkArgsCheck).apply("path/to/media_thumb.jpg")
    }
  }
}

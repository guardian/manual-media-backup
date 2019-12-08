package streamComponents

import akka.http.scaladsl.model.{ContentType, MediaType, Uri}
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import com.gu.vidispineakka.vidispine.{VSCommunicator, VSFile, VSLazyItem, VSShape}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class UploadItemShapeSpec extends Specification with Mockito {
  "UploadItemShape" should {
    "find the requested shape, open a Source to it, determine the correct filename, extension and MIME type then run the graph" in new AkkaTestkitSpecs2Support {
      implicit val vsComm = mock[VSCommunicator]
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val originalShapefile = mock[VSFile]
      originalShapefile.path returns "/path/to/mediafile.mxf"
      val originalShape = VSShape("VX-22341",1,"original","application/mxf", Seq(originalShapefile))
      val proxyShapeFile = mock[VSFile]
      proxyShapeFile.path returns "/path/to/somefile.mp4"
      val proxyShape = VSShape("VX-22342",1,"lowres","video/mp4",Seq(proxyShapeFile))
      val shapeTable = Map("original"->originalShape, "lowres"->proxyShape)
      implicit val item = new VSLazyItem("VX-1234",Map(),Some(shapeTable))

      val mockContentSource:Source[ByteString,Any] = Source.single(ByteString("File content would go here"))
      val mockCallSourceFor = mock[VSFile=>Future[Either[String, Source[ByteString, Any]]]]
      mockCallSourceFor.apply(any) returns Future(Right(mockContentSource))

      val copyGraphAssertArgs = mock[(Source[ByteString,Any], String, ContentType)=>Unit]

      val testStage = new UploadItemShape(Seq("lowres","lowaudio","lowimage"),"somebucket",CannedAcl.Private) {
        override def callSourceFor(forFile: VSFile): Future[Either[String, Source[ByteString, Any]]] = mockCallSourceFor(forFile)

        override def createCopyGraph(src: Source[ByteString, Any], fileName: String, mimeType: ContentType): RunnableGraph[Future[MultipartUploadResult]] = {
          copyGraphAssertArgs(src, fileName, mimeType)
          val sink = Sink.reduce[MultipartUploadResult]((acc,elem)=>elem)
          src
            .map(fakeContent=>MultipartUploadResult(Uri("s3://somebucket/path/to/somefile"),"somebucket","path/to/somefile",fakeContent.utf8String,None))
            .toMat(sink)(Keep.right)
        }
      }
      val expectedContentType = ContentType(MediaType.video("mp4",MediaType.NotCompressible))

      val resultFut = Source
          .single(item)
        //.fromIterator(()=>Seq(item,item).toIterator).async
        .via(testStage)
        .toMat(Sink.seq)(Keep.right)
        .run()

      val result = Await.result(resultFut,30 seconds)

      there was one(mockCallSourceFor).apply(proxyShapeFile)
      there was one(copyGraphAssertArgs).apply(mockContentSource,"/path/to/somefile.mp4",expectedContentType)

      result.headOption must beSome(item)
      result.length mustEqual 1
    }
  }
}

package streamComponents

import akka.http.scaladsl.model.{ContentType, MediaType, Uri}
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, Attributes, Materializer, Supervision}
import akka.testkit.TestProbe
import akka.util.ByteString
import com.amazonaws.services.s3.AmazonS3
import com.gu.vidispineakka.vidispine.{VSCommunicator, VSFile, VSFileState, VSLazyItem, VSMetadataEntry, VSMetadataValue, VSShape}
import helpers.CategoryPathMap
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class UploadItemShapeSpec extends Specification with Mockito {
  sequential

  "UploadItemShape" should {
    "find the requested shape, open a Source to it, determine the correct filename, extension and MIME type then run the graph" in new AkkaTestkitSpecs2Support {
      implicit val vsComm = mock[VSCommunicator]
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val originalShapefile = mock[VSFile]
      originalShapefile.state returns Some(VSFileState.CLOSED)
      originalShapefile.path returns "/path/to/mediafile.mxf"

      val originalShape = VSShape("VX-22341",1,"original","application/mxf", Seq(originalShapefile))
      val proxyShapeFile = mock[VSFile]
      proxyShapeFile.path returns "/path/to/somefile.mp4"
      proxyShapeFile.state returns Some(VSFileState.CLOSED)
      val proxyShape = VSShape("VX-22342",1,"lowres","video/mp4",Seq(proxyShapeFile))
      val shapeTable = Map("original"->originalShape, "lowres"->proxyShape)
      implicit val item = new VSLazyItem("VX-1234",Map(),Some(shapeTable))

      val mockContentSource:Source[ByteString,Any] = Source.single(ByteString("File content would go here"))
      val mockCallSourceFor = mock[VSFile=>Future[Either[String, Source[ByteString, Any]]]]
      mockCallSourceFor.apply(any) returns Future(Right(mockContentSource))

      val copyGraphAssertArgs = mock[(Source[ByteString,Any], String, ContentType)=>Unit]

      val mockS3Client = mock[AmazonS3]
      mockS3Client.doesObjectExist(any,any) returns false

      val mockLostFilesCounter = TestProbe()
      val testStage = new UploadItemShape(Seq("lowres","lowaudio","lowimage"),"somebucket",CannedAcl.Private, Some(mockLostFilesCounter.ref)) {
        override protected val s3Client = mockS3Client
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
        .fromIterator(()=>Seq(item,item).toIterator)
        .via(testStage)
        .log("streamComponents.UploadItemShape")
        .toMat(Sink.seq)(Keep.right)
        .run()

      val result = Await.result(resultFut,30 seconds)

      there were two(mockCallSourceFor).apply(proxyShapeFile)
      there were two(copyGraphAssertArgs).apply(mockContentSource,"path/to/somefile.mp4",expectedContentType)
      mockLostFilesCounter.expectNoMessage()
    }

    "fail if getContentSource fails" in new AkkaTestkitSpecs2Support {
      implicit val vsComm = mock[VSCommunicator]
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val originalShapefile = mock[VSFile]
      originalShapefile.state returns Some(VSFileState.CLOSED)
      originalShapefile.path returns "/path/to/mediafile.mxf"
      val originalShape = VSShape("VX-22341",1,"original","application/mxf", Seq(originalShapefile))
      val proxyShapeFile = mock[VSFile]
      proxyShapeFile.path returns "/path/to/somefile.mp4"
      proxyShapeFile.state returns Some(VSFileState.CLOSED)
      val proxyShape = VSShape("VX-22342",1,"lowres","video/mp4",Seq(proxyShapeFile))
      val shapeTable = Map("original"->originalShape, "lowres"->proxyShape)
      implicit val item = new VSLazyItem("VX-1234",Map(),Some(shapeTable))

      val mockCallSourceFor = mock[VSFile=>Future[Either[String, Source[ByteString, Any]]]]
      mockCallSourceFor.apply(any) returns Future.failed(new RuntimeException("Kaboom"))

      val copyGraphAssertArgs = mock[(Source[ByteString,Any], String, ContentType)=>Unit]

      val mockS3Client = mock[AmazonS3]
      mockS3Client.doesObjectExist(any,any) returns false
      val mockLostFilesCounter = TestProbe()

      val testStage = new UploadItemShape(Seq("lowres","lowaudio","lowimage"),"somebucket",CannedAcl.Private,Some(mockLostFilesCounter.ref)) {
        override protected val s3Client = mockS3Client

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
        .fromIterator(()=>Seq(item,item).toIterator)
        .via(testStage)
        .log("streamComponents.UploadItemShape")
        .toMat(Sink.seq)(Keep.right)
        .run()

      def theTest = {
        Await.result(resultFut,30 seconds)
      }

      theTest must throwA[RuntimeException]
      mockLostFilesCounter.expectNoMessage()
      there was one(mockCallSourceFor).apply(proxyShapeFile)
    }

    "fail if an output filename could not be determined" in new AkkaTestkitSpecs2Support {
      implicit val vsComm = mock[VSCommunicator]
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val originalShapefile = mock[VSFile]
      originalShapefile.state returns Some(VSFileState.CLOSED)
      originalShapefile.path returns "/path/to/mediafile.mxf"
      val originalShape = VSShape("VX-22341",1,"original","application/mxf", Seq(originalShapefile))
      val proxyShapeFile = mock[VSFile]
      proxyShapeFile.path returns "/path/to/somefile.mp4"
      proxyShapeFile.state returns Some(VSFileState.CLOSED)
      val proxyShape = VSShape("VX-22342",1,"lowres","video/mp4",Seq(proxyShapeFile))
      val shapeTable = Map("original"->originalShape, "lowres"->proxyShape)
      implicit val item = new VSLazyItem("VX-1234",Map(),Some(shapeTable))

      val mockContentSource:Source[ByteString,Any] = Source.single(ByteString("File content would go here"))
      val mockCallSourceFor = mock[VSFile=>Future[Either[String, Source[ByteString, Any]]]]
      mockCallSourceFor.apply(any) returns Future(Right(mockContentSource))

      val mockS3Client = mock[AmazonS3]
      mockS3Client.doesObjectExist(any,any) returns false
      val copyGraphAssertArgs = mock[(Source[ByteString,Any], String, ContentType)=>Unit]

      val mockLostFilesCounter = TestProbe()
      val testStage = new UploadItemShape(Seq("lowres","lowaudio","lowimage"),"somebucket",CannedAcl.Private, Some(mockLostFilesCounter.ref)) {
        override protected val s3Client = mockS3Client
        override def determineFileName(fromItem: VSLazyItem, fromShape: Option[VSShape]): Option[String] = None

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
        .fromIterator(()=>Seq(item,item).toIterator)
        .via(testStage)
        .log("streamComponents.UploadItemShape")
        .toMat(Sink.seq)(Keep.right)
        .run()

      def theTest = {
        Await.result(resultFut,30 seconds)
      }

      theTest must throwA[RuntimeException]
      mockLostFilesCounter.expectNoMessage()
      there was one(mockCallSourceFor).apply(proxyShapeFile)
    }

    "fail if no MIME type can be found" in new AkkaTestkitSpecs2Support {
      implicit val vsComm = mock[VSCommunicator]
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val originalShapefile = mock[VSFile]
      originalShapefile.state returns Some(VSFileState.CLOSED)
      originalShapefile.path returns "/path/to/mediafile.mxf"
      val originalShape = VSShape("VX-22341",1,"original","application/mxf", Seq(originalShapefile))
      val proxyShapeFile = mock[VSFile]
      proxyShapeFile.path returns "/path/to/somefile.mp4"
      proxyShapeFile.state returns Some(VSFileState.CLOSED)
      val proxyShape = VSShape("VX-22342",1,"lowres","j,hbsdfzmbzsfzfsmnbzfsinvalid-mime-type",Seq(proxyShapeFile))
      val shapeTable = Map("original"->originalShape, "lowres"->proxyShape)
      implicit val item = new VSLazyItem("VX-1234",Map(),Some(shapeTable))

      val mockContentSource:Source[ByteString,Any] = Source.single(ByteString("File content would go here"))
      val mockCallSourceFor = mock[VSFile=>Future[Either[String, Source[ByteString, Any]]]]
      mockCallSourceFor.apply(any) returns Future(Right(mockContentSource))

      val copyGraphAssertArgs = mock[(Source[ByteString,Any], String, ContentType)=>Unit]
      val mockLostFilesCounter = TestProbe()
      val mockS3Client = mock[AmazonS3]
      mockS3Client.doesObjectExist(any,any) returns false

      val testStage = new UploadItemShape(Seq("lowres","lowaudio","lowimage"),"somebucket",CannedAcl.Private, Some(mockLostFilesCounter.ref)) {
        override protected val s3Client = mockS3Client
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
        .fromIterator(()=>Seq(item,item).toIterator)
        .via(testStage)
        .log("streamComponents.UploadItemShape")
        .toMat(Sink.seq)(Keep.right)
        .run()

      def theTest = {
        Await.result(resultFut,30 seconds)
      }

      theTest must throwA[RuntimeException]
      mockLostFilesCounter.expectNoMessage()
      there was one(mockCallSourceFor).apply(proxyShapeFile)
    }

    "inform LostFilesCounter if the file is not in the right state" in new AkkaTestkitSpecs2Support {
      implicit val vsComm = mock[VSCommunicator]
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val originalShapefile = mock[VSFile]
      originalShapefile.state returns Some(VSFileState.CLOSED)
      originalShapefile.path returns "/path/to/mediafile.mxf"

      val originalShape = VSShape("VX-22341",1,"original","application/mxf", Seq(originalShapefile))
      val proxyShapeFile = mock[VSFile]
      proxyShapeFile.path returns "/path/to/somefile.mp4"
      proxyShapeFile.state returns Some(VSFileState.LOST)
      val proxyShape = VSShape("VX-22342",1,"lowres","video/mp4",Seq(proxyShapeFile))
      val shapeTable = Map("original"->originalShape, "lowres"->proxyShape)
      implicit val item = new VSLazyItem("VX-1234",Map(),Some(shapeTable))

      val mockContentSource:Source[ByteString,Any] = Source.single(ByteString("File content would go here"))
      val mockCallSourceFor = mock[VSFile=>Future[Either[String, Source[ByteString, Any]]]]
      mockCallSourceFor.apply(any) returns Future(Right(mockContentSource))

      val copyGraphAssertArgs = mock[(Source[ByteString,Any], String, ContentType)=>Unit]
      val mockS3Client = mock[AmazonS3]
      mockS3Client.doesObjectExist(any,any) returns false

      val mockLostFilesCounter = TestProbe()
      val testStage = new UploadItemShape(Seq("lowres","lowaudio","lowimage"),"somebucket",CannedAcl.Private, Some(mockLostFilesCounter.ref)) {
        override protected val s3Client = mockS3Client
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
        .fromIterator(()=>Seq(item,item).toIterator)
        .via(testStage)
        .log("streamComponents.UploadItemShape")
        .toMat(Sink.seq)(Keep.right)
        .run()

      val result = Await.result(resultFut,30 seconds)

      there were no(mockCallSourceFor).apply(any)
      there were no(copyGraphAssertArgs).apply(any,any,any)
      mockLostFilesCounter.expectMsgAllClassOf(classOf[LostFilesCounter.RegisterLost])
      mockLostFilesCounter.expectMsg(LostFilesCounter.RegisterLost(proxyShapeFile,proxyShape,item))
    }

    "apply a path prefix if given by the provided StoragePathMap" in new AkkaTestkitSpecs2Support {
      implicit val vsComm = mock[VSCommunicator]
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val originalShapefile = mock[VSFile]
      originalShapefile.state returns Some(VSFileState.CLOSED)
      originalShapefile.path returns "/path/to/mediafile.mxf"

      val originalShape = VSShape("VX-22341",1,"original","application/mxf", Seq(originalShapefile))
      val proxyShapeFile = mock[VSFile]
      proxyShapeFile.path returns "/path/to/somefile.mp4"
      proxyShapeFile.state returns Some(VSFileState.CLOSED)
      val proxyShape = VSShape("VX-22342",1,"lowres","video/mp4",Seq(proxyShapeFile))
      val shapeTable = Map("original"->originalShape, "lowres"->proxyShape)
      implicit val item = new VSLazyItem("VX-1234",
        Map("gnm_asset_category"->VSMetadataEntry("gnm_asset_category",None,None,None,None,Seq(VSMetadataValue("category-name",None,None,None,None)))),
      Some(shapeTable))

      val mockContentSource:Source[ByteString,Any] = Source.single(ByteString("File content would go here"))
      val mockCallSourceFor = mock[VSFile=>Future[Either[String, Source[ByteString, Any]]]]
      mockCallSourceFor.apply(any) returns Future(Right(mockContentSource))

      val copyGraphAssertArgs = mock[(Source[ByteString,Any], String, ContentType)=>Unit]

      val mockS3Client = mock[AmazonS3]
      mockS3Client.doesObjectExist(any,any) returns false

      val spMap = mock[CategoryPathMap]
      spMap.pathPrefixForStorage(any) returns Some("path/prefix")

      val mockLostFilesCounter = TestProbe()
      val testStage = new UploadItemShape(Seq("lowres","lowaudio","lowimage"),"somebucket",CannedAcl.Private, Some(mockLostFilesCounter.ref), Some(spMap)) {
        override protected val s3Client = mockS3Client
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
        .fromIterator(()=>Seq(item,item).toIterator)
        .via(testStage)
        .log("streamComponents.UploadItemShape")
        .toMat(Sink.seq)(Keep.right)
        .run()

      val result = Await.result(resultFut,30 seconds)

      there were two(mockCallSourceFor).apply(proxyShapeFile)
      there were two(copyGraphAssertArgs).apply(mockContentSource,"path/prefix/path/to/somefile.mp4",expectedContentType)
      mockLostFilesCounter.expectNoMessage()
    }

  }

}

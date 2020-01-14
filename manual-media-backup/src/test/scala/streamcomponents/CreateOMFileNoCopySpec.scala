package streamcomponents

import java.io.File

import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import com.om.mxs.client.japi.{MxsObject, UserInfo, Vault}
import models.{BackupEntry, MxsMetadata, ObjectMatrixEntry}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import akka.stream.scaladsl.GraphDSL.Implicits._

import scala.concurrent.Await
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

class CreateOMFileNoCopySpec extends Specification with Mockito {
  "CreateOMFileNoCopy" should {
    "lift filesystem metadata for the given Path, call out to MatrixStore to create the object and update the BackupEntry" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val fakeUserInfo = mock[UserInfo]
      val fakeVault = mock[Vault]

      val createdObject = mock[MxsObject]
      createdObject.getId returns "12345678"
      val filesystemMeta = MxsMetadata.empty()
        .withString("MXFS_FILENAME","someFilename")
        .withString("MXFS_PATH","/path/to/someFilename")
        .withString("MXFS_FILENAME_UPPER", "SOMEFILENAME")
        .withValue("GNM_BEING_WRITTEN", true)

      val mockedMetadataFromFilesystem = mock[File=>Try[MxsMetadata]]
      mockedMetadataFromFilesystem.apply(any) returns Success(filesystemMeta)

      val mockedCreateObject = mock[(Option[String],File,MxsMetadata)=>Try[(MxsObject,MxsMetadata)]]
      mockedCreateObject.apply(any,any,any) returns Success((createdObject, filesystemMeta))

      val toTest = new CreateOMFileNoCopy(fakeUserInfo) {
        override def callOpenVault: Some[Vault] = Some(fakeVault)

        override def callCreateObjectWithMetadata(filePath: Option[String], srcFile: File, meta: MxsMetadata)(implicit v: Vault) = mockedCreateObject(filePath, srcFile, meta)

        override def callMetadataFromFilesystem(srcFile: File): Try[MxsMetadata] = mockedMetadataFromFilesystem(srcFile)
      }
      val f = new File("/path/to/someFilename").toPath
      val incomingEntry = BackupEntry(f, None)

      val sinkFac = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sinkFac) { implicit builder=> sink=>
        val src = Source.single(incomingEntry)
        val createFile = builder.add(toTest)

        src ~> createFile ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      result.length mustEqual 1
      result.head.originalPath mustEqual incomingEntry.originalPath
      result.head.maybeObjectMatrixEntry must beSome
      result.head.maybeObjectMatrixEntry.get.oid mustEqual "12345678"
      there was one(mockedMetadataFromFilesystem).apply(f.toFile)
      there was one(mockedCreateObject).apply(Some("/path/to/someFilename"), f.toFile, filesystemMeta)
    }

    "not attempt to create another instance of an object that exists" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val fakeUserInfo = mock[UserInfo]
      val fakeVault = mock[Vault]

      val createdObject = mock[MxsObject]
      createdObject.getId returns "12345678"
      val filesystemMeta = MxsMetadata.empty()
        .withString("MXFS_FILENAME","someFilename")
        .withString("MXFS_PATH","/path/to/someFilename")
        .withString("MXFS_FILENAME_UPPER", "SOMEFILENAME")
        .withValue("GNM_BEING_WRITTEN", true)

      val mockedMetadataFromFilesystem = mock[File=>Try[MxsMetadata]]
      mockedMetadataFromFilesystem.apply(any) returns Success(filesystemMeta)

      val mockedCreateObject = mock[(Option[String],File,MxsMetadata)=>Try[(MxsObject,MxsMetadata)]]
      mockedCreateObject.apply(any,any,any) returns Success((createdObject, filesystemMeta))

      val toTest = new CreateOMFileNoCopy(fakeUserInfo) {
        override def callOpenVault: Some[Vault] = Some(fakeVault)

        override def callCreateObjectWithMetadata(filePath: Option[String], srcFile: File, meta: MxsMetadata)(implicit v: Vault) = mockedCreateObject(filePath, srcFile, meta)

        override def callMetadataFromFilesystem(srcFile: File): Try[MxsMetadata] = mockedMetadataFromFilesystem(srcFile)
      }
      val f = new File("/path/to/someFilename").toPath
      val incomingEntry = BackupEntry(f, Some(ObjectMatrixEntry("99887766", None,None)))

      val sinkFac = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sinkFac) { implicit builder=> sink=>
        val src = Source.single(incomingEntry)
        val createFile = builder.add(toTest)

        src ~> createFile ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      result.length mustEqual 1
      result.head.originalPath mustEqual incomingEntry.originalPath
      result.head.maybeObjectMatrixEntry must beSome
      result.head.maybeObjectMatrixEntry.get.oid mustEqual "99887766"
      there was no(mockedMetadataFromFilesystem).apply(any)
      there was no(mockedCreateObject).apply(any, any, any)
    }

    "fail if the create operation fails" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val fakeUserInfo = mock[UserInfo]
      val fakeVault = mock[Vault]

      val createdObject = mock[MxsObject]
      createdObject.getId returns "12345678"
      val filesystemMeta = MxsMetadata.empty()
        .withString("MXFS_FILENAME","someFilename")
        .withString("MXFS_PATH","/path/to/someFilename")
        .withString("MXFS_FILENAME_UPPER", "SOMEFILENAME")
        .withValue("GNM_BEING_WRITTEN", true)

      val mockedMetadataFromFilesystem = mock[File=>Try[MxsMetadata]]
      mockedMetadataFromFilesystem.apply(any) returns Success(filesystemMeta)

      val mockedCreateObject = mock[(Option[String],File,MxsMetadata)=>Try[(MxsObject,MxsMetadata)]]
      mockedCreateObject.apply(any,any,any) returns Failure(new RuntimeException("Kaboom!"))

      val toTest = new CreateOMFileNoCopy(fakeUserInfo) {
        override def callOpenVault: Some[Vault] = Some(fakeVault)

        override def callCreateObjectWithMetadata(filePath: Option[String], srcFile: File, meta: MxsMetadata)(implicit v: Vault) = mockedCreateObject(filePath, srcFile, meta)

        override def callMetadataFromFilesystem(srcFile: File): Try[MxsMetadata] = mockedMetadataFromFilesystem(srcFile)
      }
      val f = new File("/path/to/someFilename").toPath
      val incomingEntry = BackupEntry(f,  None)

      val sinkFac = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sinkFac) { implicit builder=> sink=>
        val src = Source.single(incomingEntry)
        val createFile = builder.add(toTest)

        src ~> createFile ~> sink
        ClosedShape
      }

      def theTest {
        Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      }
      theTest must throwA[RuntimeException]("Kaboom!")
      there was one(mockedMetadataFromFilesystem).apply(any)
      there was one(mockedCreateObject).apply(any, any, any)
    }
  }
}

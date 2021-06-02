package streamcomponents

import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import com.om.mxs.client.japi.{UserInfo, Vault}
import models.{FileAttributes, FileEntry, LocalFile, MxsMetadata, ObjectMatrixEntry, RemoteFile}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

import java.nio.file.Paths
import java.time.ZonedDateTime
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Success, Try}

class FindRemoteFileSpec extends Specification with Mockito with AfterAll {
  implicit val system:ActorSystem = ActorSystem()
  implicit val mat:Materializer = ActorMaterializer.create(system)

  override def afterAll() = {
    Await.result(system.terminate(), 10.seconds)
  }

  "FindRemoteFile" should {
    "create a FileEntry object if there is a matching file" in {
      val mockVault = mock[Vault]
      val mockUserInfo = mock[UserInfo]
      val mockOpenVault = mock[UserInfo => Vault]
      mockOpenVault.apply(any) returns mockVault
      val mockFindByFilename = mock[(Vault, String) => Try[Seq[ObjectMatrixEntry]]]
      val mockResult = ObjectMatrixEntry("some-oid", Some(MxsMetadata.empty().withValue("__mxs__length", 9999L)), None)
      val mockGetMd5 = mock[(Vault, String, Int, Int)=>Try[String]]
      mockGetMd5.apply(any,any,any,any) returns Success("remote-md5")
      mockFindByFilename.apply(any, any) returns Success(Seq(mockResult))

      val toTest = new FindRemoteFile(mockUserInfo) {
        override def callOpenVault(userInfo: UserInfo): Vault = mockOpenVault(userInfo)

        override def callFindByFilename(vault: Vault, filepath: String): Try[Seq[ObjectMatrixEntry]] = mockFindByFilename(vault, filepath)

        override def getMd5(vault: Vault, oid: String, attempt: Int, maxAttempts: Int): Try[String] = mockGetMd5(vault, oid, attempt, maxAttempts)
      }

      val graph = GraphDSL.create(Sink.seq[FileEntry]) { implicit builder=>sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._
        val src = builder.add(Source.single(FileEntry(LocalFile(Paths.get("/path/to/some/entry"), 123,None), None)))
        val lookup = builder.add(toTest)
        src ~> lookup ~> sink
        ClosedShape
      }
      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 5.seconds)

      result.length mustEqual 1

      there was one(mockOpenVault).apply(mockUserInfo)
      there was one(mockFindByFilename).apply(mockVault, "/path/to/some/entry")
      there was one(mockGetMd5).apply(mockVault, "some-oid",0, 20)
      there was one(mockVault).dispose()

      result.head.localFile mustEqual LocalFile(Paths.get("/path/to/some/entry"), 123,None)
      result.head.remoteFile must beSome(
        RemoteFile(
          "some-oid",
          9999,
          Some("remote-md5")
        )
      )
    }
  }
}

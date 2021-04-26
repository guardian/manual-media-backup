import akka.actor.ActorSystem
import akka.stream.Materializer
import com.om.mxs.client.japi.Vault
import models.{FileInstance, MxsMetadata, PathTransform, PathTransformSet, ToCopy}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.io.File
import java.nio.file.{Path, Paths}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Success, Try}

class DirectCopierSpec extends Specification with Mockito {
  "DirectCopier.performCopy" should {
    "call doCopyTo for each identified file that needs copying in the input" in {
      implicit val mockActorSystem = mock[ActorSystem]
      implicit val mockMaterializer = mock[Materializer]
      val mockVault = mock[Vault]
      val mockDoCopyTo = mock[(Vault, Option[String], File, Int, String, String, Boolean, Boolean)=>Future[(String, Option[String])]]
      mockDoCopyTo(any,any,any,any,any,any,any,any) returns Future(("object-id",Some("valid-checksum")))
      val mockCheckFileExistence = mock[Path=>Try[Boolean]]
      mockCheckFileExistence.apply(any) returns Success(false)

      val toTest = new DirectCopier(mockVault, new PathTransformSet(Seq())) {
        override def checkFileExistence(remotePath: Path): Try[Boolean] = mockCheckFileExistence(remotePath)
        override def doCopyTo(vault: Vault, destFileName: Option[String], fromFile: File, chunkSize: Int, gnmTypeMeta:String, checksumType: String, keepOnFailure: Boolean, retryOnFailure: Boolean, externalMetadata:Option[MxsMetadata])(implicit ec: ExecutionContext, mat: Materializer): Future[(String, Option[String])] =
          mockDoCopyTo(vault, destFileName, fromFile, chunkSize, gnmTypeMeta,checksumType, keepOnFailure, retryOnFailure)
      }

      val result = Await.result(toTest.performCopy(ToCopy(
        Paths.get("/srv/volume/Media/project/content/somefile.mxf"),
        Some(Paths.get("/srv/volume/Proxies/project/content/somefile.mp4")),
        Some(Paths.get("/srv/volume/Proxies/project/content/somefile.jpg")),
      )), 10 seconds)

      result.sourceFile.oid must beSome("object-id")
      result.sourceFile.omChecksum must beSome("valid-checksum")
      there were three(mockDoCopyTo).apply(any,any,any,any,any,any,any,any)
      there were three(mockCheckFileExistence).apply(any)

      there was one(mockDoCopyTo).apply(mockVault,
        None,
        new File("/srv/volume/Media/project/content/somefile.mxf"),
        toTest.defaultChunkSize,
        "Rushes",
        "md5",false, false)
      there was one(mockDoCopyTo).apply(mockVault,
        None,
        new File("/srv/volume/Proxies/project/content/somefile.mp4"),
        toTest.defaultChunkSize,
        "Proxy",
        "md5",false, false)
      there was one(mockDoCopyTo).apply(mockVault,
        None,
        new File("/srv/volume/Proxies/project/content/somefile.jpg"),
        toTest.defaultChunkSize,
        "Poster",
        "md5",false, false)
      there was one(mockCheckFileExistence).apply(Paths.get("/srv/volume/Media/project/content/somefile.mxf"))
    }

    "apply any path transformers that are specified" in {
      implicit val mockActorSystem = mock[ActorSystem]
      implicit val mockMaterializer = mock[Materializer]

      val pathTransformers = new PathTransformSet(Seq(
        PathTransform("/srv/volume/Media","/srv/othervolume/media"),
        PathTransform("/srv/volume/Proxies", "/srv/othervolume/proxies"),
      ))

      val mockVault = mock[Vault]
      val mockDoCopyTo = mock[(Vault, Option[String], File, Int, String, Boolean, Boolean)=>Future[(String, Option[String])]]
      mockDoCopyTo(any,any,any,any,any,any,any) returns Future(("object-id",Some("valid-checksum")))
      val mockCheckFileExistence = mock[Path=>Try[Boolean]]
      mockCheckFileExistence.apply(any) returns Success(false)

      val toTest = new DirectCopier(mockVault, pathTransformers) {
        override def checkFileExistence(remotePath: Path): Try[Boolean] = mockCheckFileExistence(remotePath)
        override def doCopyTo(vault: Vault, destFileName: Option[String], fromFile: File, chunkSize: Int, gnmTypeMeta:String, checksumType: String, keepOnFailure: Boolean, retryOnFailure: Boolean, externalMetadata:Option[MxsMetadata])(implicit ec: ExecutionContext, mat: Materializer): Future[(String, Option[String])] =
          mockDoCopyTo(vault, destFileName, fromFile, chunkSize, checksumType, keepOnFailure, retryOnFailure)
      }

      val result = Await.result(toTest.performCopy(ToCopy(
        Paths.get("/srv/volume/Media/project/content/somefile.mxf"),
        Some(Paths.get("/srv/volume/Proxies/project/content/somefile.mp4")),
        Some(Paths.get("/srv/volume/Proxies/project/content/somefile.jpg")),
      )), 10 seconds)

      result.sourceFile.oid must beSome("object-id")
      result.sourceFile.omChecksum must beSome("valid-checksum")
      there were three(mockDoCopyTo).apply(any,any,any,any,any,any,any)
      there were three(mockCheckFileExistence).apply(any)

      there was one(mockCheckFileExistence).apply(Paths.get("/srv/othervolume/media/project/content/somefile.mxf"))
      there was one(mockCheckFileExistence).apply(Paths.get("/srv/othervolume/proxies/project/content/somefile.mp4"))
      there was one(mockCheckFileExistence).apply(Paths.get("/srv/othervolume/proxies/project/content/somefile.jpg"))

      there was one(mockDoCopyTo).apply(mockVault,
        Some("/srv/othervolume/media/project/content/somefile.mxf"),
        new File("/srv/volume/Media/project/content/somefile.mxf"),
        toTest.defaultChunkSize,
        "md5",false, false)

      there was one(mockDoCopyTo).apply(mockVault,
        Some("/srv/othervolume/proxies/project/content/somefile.mp4"),
        new File("/srv/volume/Proxies/project/content/somefile.mp4"),
        toTest.defaultChunkSize,
        "md5",false, false)
    }

    "not attempt a copy if the file already exists" in {
      implicit val mockActorSystem = mock[ActorSystem]
      implicit val mockMaterializer = mock[Materializer]
      val mockVault = mock[Vault]
      val mockDoCopyTo = mock[(Vault, Option[String], File, Int, String, Boolean, Boolean)=>Future[(String, Option[String])]]
      mockDoCopyTo(any,any,any,any,any,any,any) returns Future(("object-id",Some("valid-checksum")))
      val mockCheckFileExistence = mock[Path=>Try[Boolean]]
      mockCheckFileExistence.apply(Paths.get("/srv/volume/Media/project/content/somefile.mxf")) returns Success(true)
      mockCheckFileExistence.apply(Paths.get("/srv/volume/Proxies/project/content/somefile.mp4")) returns Success(false)
      mockCheckFileExistence.apply(Paths.get("/srv/volume/Proxies/project/content/somefile.jpg")) returns Success(false)

      val toTest = new DirectCopier(mockVault, new PathTransformSet(Seq())) {
        override def checkFileExistence(remotePath: Path): Try[Boolean] = mockCheckFileExistence(remotePath)
        override def doCopyTo(vault: Vault, destFileName: Option[String], fromFile: File, chunkSize: Int, gnmTypeMeta:String, checksumType: String, keepOnFailure: Boolean, retryOnFailure: Boolean, externalMetadata:Option[MxsMetadata])(implicit ec: ExecutionContext, mat: Materializer): Future[(String, Option[String])] =
          mockDoCopyTo(vault, destFileName, fromFile, chunkSize, checksumType, keepOnFailure, retryOnFailure)
      }

      val result = Await.result(toTest.performCopy(ToCopy(
        Paths.get("/srv/volume/Media/project/content/somefile.mxf"),
        Some(Paths.get("/srv/volume/Proxies/project/content/somefile.mp4")),
        Some(Paths.get("/srv/volume/Proxies/project/content/somefile.jpg")),
      )), 10 seconds)

      result.sourceFile.oid must beNone
      result.sourceFile.omChecksum must beNone
      result.proxyMedia.flatMap(_.oid) must beSome("object-id")
      result.proxyMedia.flatMap(_.omChecksum) must beSome("valid-checksum")
      there were two(mockDoCopyTo).apply(any,any,any,any,any,any,any)
      there were three(mockCheckFileExistence).apply(any)

      there was no(mockDoCopyTo).apply(mockVault,
        None,
        new File("/srv/volume/Media/project/content/somefile.mxf"),
        toTest.defaultChunkSize,
        "md5",false, false)
      there was one(mockDoCopyTo).apply(mockVault,
        None,
        new File("/srv/volume/Proxies/project/content/somefile.mp4"),
        toTest.defaultChunkSize,
        "md5",false, false)
      there was one(mockDoCopyTo).apply(mockVault,
        None,
        new File("/srv/volume/Proxies/project/content/somefile.jpg"),
        toTest.defaultChunkSize,
        "md5",false, false)
      there was one(mockCheckFileExistence).apply(Paths.get("/srv/volume/Media/project/content/somefile.mxf"))
    }
  }

  "DirectCopier.addCopiedOIDs" should {
    "return a Future containing an updated ToCopy object" in {
      val testData:Future[Seq[(String, Option[String])]] = Future(Seq(
        ("source-media-oid", Some("source-media-cs")),
        ("proxy-media-oid", Some("proxy-media-cs")),
        ("thumb-media-oid", Some("thumb-media-cs")),
      ))

      val incomingData = ToCopy(
        FileInstance(
          Paths.get("path/to/some/media.mxf"),
          None,
          None
        ),
        Some(FileInstance(
          Paths.get("path/to/some/proxy.mp4"),
          None,
          None
        )),
        Some(FileInstance(
          Paths.get("path/to/some/thumbnail.jpg"),
          None,
          None
        )),
        None
      )

      val toTest = new DirectCopier(mock[Vault], PathTransformSet.empty)
      val result = Await.result(toTest.addCopiedOIDs(testData, incomingData), 1.seconds)

      result.sourceFile.oid must beSome("source-media-oid")
      result.sourceFile.omChecksum must beSome("source-media-cs")
      result.proxyMedia.flatMap(_.oid) must beSome("proxy-media-oid")
      result.proxyMedia.flatMap(_.omChecksum) must beSome("proxy-media-cs")
      result.thumbnail.flatMap(_.oid) must beSome("thumb-media-oid")
      result.thumbnail.flatMap(_.omChecksum) must beSome("thumb-media-cs")
    }
  }
}

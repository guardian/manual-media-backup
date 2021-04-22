import akka.actor.ActorSystem
import akka.stream.Materializer
import com.om.mxs.client.japi.Vault
import models.{PathTransform, ToCopy}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.io.File
import java.nio.file.Paths
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DirectCopierSpec extends Specification with Mockito {
  "DirectCopier.performCopy" should {
    "call doCopyTo for each identified file that needs copying in the input" in {
      implicit val mockActorSystem = mock[ActorSystem]
      implicit val mockMaterializer = mock[Materializer]
      val mockVault = mock[Vault]
      val mockDoCopyTo = mock[(Vault, Option[String], File, Int, String, Boolean, Boolean)=>Future[(String, Option[String])]]
      mockDoCopyTo(any,any,any,any,any,any,any) returns Future(("object-id",Some("valid-checksum")))

      val toTest = new DirectCopier(mockVault, Seq()) {
        override def doCopyTo(vault: Vault, destFileName: Option[String], fromFile: File, chunkSize: Int, checksumType: String, keepOnFailure: Boolean, retryOnFailure: Boolean)(implicit ec: ExecutionContext, mat: Materializer): Future[(String, Option[String])] =
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

      there was one(mockDoCopyTo).apply(mockVault,
        None,
        new File("/srv/volume/Media/project/content/somefile.mxf"),
        toTest.defaultChunkSize,
        "md5",false, false)
    }

    "apply any path transformers that are specified" in {
      implicit val mockActorSystem = mock[ActorSystem]
      implicit val mockMaterializer = mock[Materializer]

      val pathTransformers = Seq(
        PathTransform("/srv/volume/Media","/srv/othervolume/media"),
        PathTransform("/srv/volume/Proxies", "/srv/othervolume/proxies"),
      )

      val mockVault = mock[Vault]
      val mockDoCopyTo = mock[(Vault, Option[String], File, Int, String, Boolean, Boolean)=>Future[(String, Option[String])]]
      mockDoCopyTo(any,any,any,any,any,any,any) returns Future(("object-id",Some("valid-checksum")))

      val toTest = new DirectCopier(mockVault, pathTransformers) {
        override def doCopyTo(vault: Vault, destFileName: Option[String], fromFile: File, chunkSize: Int, checksumType: String, keepOnFailure: Boolean, retryOnFailure: Boolean)(implicit ec: ExecutionContext, mat: Materializer): Future[(String, Option[String])] =
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
  }
}

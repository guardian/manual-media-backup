import com.om.mxs.client.japi.{Attribute, MxsObject, ObjectTypedAttributeView, Vault}
import models.{FileInstance, ToCopy}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.nio.file.Paths
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try

class ProxyLinkerSpec extends Specification with Mockito {
  "ProxyLinker.performLinkup" should {
    "call writeAttribute for every attribute required to be written" in {
      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject
      val mockAttrView = mock[ObjectTypedAttributeView]
      mockObject.getAttributeView returns mockAttrView

      val input = ToCopy(
        FileInstance(
          Paths.get("some/path/to/mediafile.mxf"),
          Some("main-media-oid"),
          Some("checksum-here")
        ),
        Some(
          FileInstance(
            Paths.get("some/path/to/proxy.mp4"),
            Some("proxy-media-oid"),
            Some("checksum-here")
          )
        ),
        Some(
          FileInstance(
            Paths.get("some/path/to/thumbnail.jpg"),
            Some("thumbnail-media-oid"),
            Some("checksum-here")
          )
        ),
        None
      )

      val toTest = new ProxyLinker(mockVault)
      val result = Try { Await.result(toTest.performLinkup(input), 10.seconds) }

      there was one(mockVault).getObject("main-media-oid")
      there was one(mockObject).getAttributeView
      there was one(mockAttrView).writeAttribute(new Attribute("ATT_PROXY_OID","proxy-media-oid"))
      there was one(mockAttrView).writeAttribute(new Attribute("ATT_THUMB_OID", "thumbnail-media-oid"))
      there were two(mockAttrView).writeAttribute(any)
      result must beSuccessfulTry
    }

    "skip attributes that should not be set" in {
      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject
      val mockAttrView = mock[ObjectTypedAttributeView]
      mockObject.getAttributeView returns mockAttrView

      val input = ToCopy(
        FileInstance(
          Paths.get("some/path/to/mediafile.mxf"),
          Some("main-media-oid"),
          Some("checksum-here")
        ),
        None,
        Some(
          FileInstance(
            Paths.get("some/path/to/thumbnail.jpg"),
            Some("thumbnail-media-oid"),
            Some("checksum-here")
          )
        ),
        None
      )

      val toTest = new ProxyLinker(mockVault)
      val result = Try { Await.result(toTest.performLinkup(input), 10.seconds) }

      there was one(mockVault).getObject("main-media-oid")
      there was one(mockObject).getAttributeView
      there was one(mockAttrView).writeAttribute(new Attribute("ATT_THUMB_OID", "thumbnail-media-oid"))
      there was one(mockAttrView).writeAttribute(any)
      result must beSuccessfulTry
    }

    "return the input unchanged if there is no main mediaoid set" in {
      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject
      val mockAttrView = mock[ObjectTypedAttributeView]
      mockObject.getAttributeView returns mockAttrView

      val input = ToCopy(
        FileInstance(
          Paths.get("some/path/to/mediafile.mxf"),
          None,
          None
        ),
        None,
        Some(
          FileInstance(
            Paths.get("some/path/to/thumbnail.jpg"),
            Some("thumbnail-media-oid"),
            Some("checksum-here")
          )
        ),
        None
      )

      val toTest = new ProxyLinker(mockVault)
      val result = Try { Await.result(toTest.performLinkup(input), 10.seconds) }

      there was no(mockVault).getObject("main-media-oid")
      there was no(mockObject).getAttributeView
      there was no(mockAttrView).writeAttribute(any)
      result must beSuccessfulTry
      result.get mustEqual input
    }
  }
}

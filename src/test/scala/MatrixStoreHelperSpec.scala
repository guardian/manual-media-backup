import java.io.File

import com.om.mxs.client.japi.{MxsObject, ObjectTypedAttributeView}
import helpers.MatrixStoreHelper
import models.MxsMetadata
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.Await
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

class MatrixStoreHelperSpec extends Specification with Mockito {
  "MatrixStoreHelper.getFileExt" should {
    "extract the file extension from a string" in {
      val result = MatrixStoreHelper.getFileExt("filename.ext")
      result must beSome("ext")
    }

    "return None if there is no file extension" in {
      val result = MatrixStoreHelper.getFileExt("filename")
      result must beNone
    }

    "return None if there is a dot but no file extension" in {
      val result = MatrixStoreHelper.getFileExt("filename.")
      result must beNone
    }

    "filter out something that is too long for an extension" in {
      val result = MatrixStoreHelper.getFileExt("filename.somethingreallylong")
      result must beNone
    }
  }

  "MatrixStoreHelper.metadataFromFilesystem" should {
    "look up filesystem metadata and convert it to MxsMetadata" in {
      val result = MatrixStoreHelper.metadataFromFilesystem(new File("build.sbt"))
      println(result.toString)
      result must beSuccessfulTry
    }
  }

  "MatrixStoreHelper.getOMFileMD5" should {
    "request the specific MD5 key and return it" in {
      val mockedMetadataView = mock[ObjectTypedAttributeView]
      mockedMetadataView.readString("__mxs__calc_md5") returns "adff7d2ede6489c4"

      val mockedMxsObject = mock[MxsObject]
      mockedMxsObject.getAttributeView returns mockedMetadataView

      val result = Await.result(MatrixStoreHelper.getOMFileMd5(mockedMxsObject), 30 seconds)
      there was one(mockedMetadataView).readString("__mxs__calc_md5")
      result must beSuccessfulTry("61646666376432656465363438396334") //the return value of readString is a binary string, which is converted to hex representation/
    }

    "pass back an exception as a failed try" in {
      val mockedMetadataView = mock[ObjectTypedAttributeView]
      val fakeException = new RuntimeException("aaaarg!")
      mockedMetadataView.readString("__mxs__calc_md5") throws fakeException

      val mockedMxsObject = mock[MxsObject]
      mockedMxsObject.getAttributeView returns mockedMetadataView

      val result = Await.result(MatrixStoreHelper.getOMFileMd5(mockedMxsObject), 30 seconds)
      there was one(mockedMetadataView).readString("__mxs__calc_md5")
      result must beFailedTry(fakeException)
    }

    "retry until a result is given" in {
      val mockedMetadataView = mock[ObjectTypedAttributeView]
      mockedMetadataView.readString("__mxs__calc_md5") returns "" thenReturn "" thenReturn "adff7d2ede6489c4"

      val mockedMxsObject = mock[MxsObject]
      mockedMxsObject.getAttributeView returns mockedMetadataView

      val result = Await.result(MatrixStoreHelper.getOMFileMd5(mockedMxsObject), 30 seconds)
      there were three(mockedMetadataView).readString("__mxs__calc_md5")
      result must beSuccessfulTry("61646666376432656465363438396334")
    }
  }
}

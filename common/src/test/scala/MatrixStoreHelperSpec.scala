import java.io.File
import java.nio.ByteBuffer

import com.om.mxs.client.japi.{MxsObject, ObjectTypedAttributeView}
import helpers.MatrixStoreHelper
import models.MxsMetadata
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.mockito.ArgumentMatchers._
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
      //mockedMetadataView.readString("__mxs__calc_md5") returns "adff7d2ede6489c4"
      mockedMetadataView.read(anyString,any[ByteBuffer]) answers((args:Array[AnyRef])=>{
        val buffer = args(1).asInstanceOf[ByteBuffer]
        buffer.put("adff7d2ede6489c4".getBytes)
        "adff7d2ede6489c4".length
      })
      val mockedMxsObject = mock[MxsObject]
      mockedMxsObject.getAttributeView returns mockedMetadataView
      mockedMxsObject.getId returns "some-objectmatrix-id"
      val result = Await.result(MatrixStoreHelper.getOMFileMd5(mockedMxsObject), 30 seconds)
      there was one(mockedMetadataView).read(org.mockito.ArgumentMatchers.eq("__mxs__calc_md5"),org.mockito.ArgumentMatchers.any[ByteBuffer])
      result must beSuccessfulTry("61646666376432656465363438396334") //the return value of readString is a binary string, which is converted to hex representation/
    }

    "pass back an exception as a failed try" in {
      val mockedMetadataView = mock[ObjectTypedAttributeView]
      val fakeException = new RuntimeException("aaaarg!")
      mockedMetadataView.read(anyString,any) throws fakeException

      val mockedMxsObject = mock[MxsObject]
      mockedMxsObject.getAttributeView returns mockedMetadataView

      val result = Await.result(MatrixStoreHelper.getOMFileMd5(mockedMxsObject), 30 seconds)
      there was one(mockedMetadataView).read(org.mockito.ArgumentMatchers.eq("__mxs__calc_md5"),org.mockito.ArgumentMatchers.any[ByteBuffer])
      result must beFailedTry(fakeException)
    }

    "retry until a result is given" in {
      val mockedMetadataView = mock[ObjectTypedAttributeView]
      var ctr=0
      mockedMetadataView.read(anyString,any[ByteBuffer]) answers((args:Array[AnyRef])=>{
        val buffer = args(1).asInstanceOf[ByteBuffer]
        ctr+=1
        if(ctr<3){
          buffer.clear()
          0
        } else {
          buffer.put("adff7d2ede6489c4".getBytes)
          "adff7d2ede6489c4".length
        }
      })


      val mockedMxsObject = mock[MxsObject]
      mockedMxsObject.getAttributeView returns mockedMetadataView

      val result = Await.result(MatrixStoreHelper.getOMFileMd5(mockedMxsObject), 30 seconds)
      there were three(mockedMetadataView).read(org.mockito.ArgumentMatchers.eq("__mxs__calc_md5"),org.mockito.ArgumentMatchers.any[ByteBuffer])
      result must beSuccessfulTry("61646666376432656465363438396334")
    }
  }

  "MatrixStoreHelper.santiseFileNameForQuery" should {
    "pass through a string with no special chars unchanged" in {
      val result = MatrixStoreHelper.santiseFileNameForQuery("somesimplenormalfilename.ext")
      result mustEqual "somesimplenormalfilename.ext"
    }

    "escape doublequotes" in {
      val result = MatrixStoreHelper.santiseFileNameForQuery("""file name with \" character""")
      result mustEqual("""file name with \\\" character""")
    }

    "escape some other nasties" in {
      val result = MatrixStoreHelper.santiseFileNameForQuery("path/to/some/extrémely &!|{} wrong filename!!!")
      result mustEqual "path/to/some/extrémely \\&\\!\\|\\{\\} wrong filename\\!\\!\\!"
    }
  }
}

import java.io.File

import helpers.MatrixStoreHelper
import models.MxsMetadata
import org.specs2.mutable.Specification

class MatrixStoreHelperSpec extends Specification {
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
//      (MxsMetadata(
//        Map("MXFS_FILENAME_UPPER" -> "BUILD.SBT",
//          "MXFS_USERNAME" -> "test",
//          "MXFS_PATH" -> "/Users/localhome/workdev/manual-media-backup/build.sbt",
//          "MXFS_FILENAME" -> "build.sbt", "MXFS_MIMETYPE" -> null),
//        Map(),
//        Map("DPSP_SIZE" -> 607, "MXFS_MODIFICATION_TIME" -> 1560356037000L, "MXFS_CREATION_TIME" -> 1560356037000L, "MXFS_ACCESS_TIME" -> 1560413777000L),
//        Map("MXFS_CREATIONDAY" -> 12, "MXFS_ARCHMONTH" -> 6, "MXFS_COMPATIBLE" -> 1, "MXFS_ARCHYEAR" -> 2019,
//          "MXFS_ARCHDAY" -> 13, "MXFS_ARCHIVE_TIME" -> 725000000, "MXFS_CREATIONMONTH" -> 6, "MXFS_CREATIONYEAR" -> 2019)
//      )
//      )

    }
  }
}

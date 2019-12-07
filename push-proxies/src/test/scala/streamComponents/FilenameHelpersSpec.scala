package streamComponents

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import com.gu.vidispineakka.vidispine.{VSFile, VSLazyItem, VSShape}

class FilenameHelpersSpec extends Specification with Mockito {
  class TestClass extends FilenameHelpers

  "FilenameHelpers.getExtension" should {
    "retrieve the file extension for a path" in {
      val test = new TestClass
      val result = test.getExtension("/path/to/some/file.ext")

      result must beSome("ext")
    }

    "returnNone if there is no file extension" in {
      val test = new TestClass
      val result = test.getExtension("/path/to/some/file")

      result must beNone
    }

    "not break if there is no filename" in {
      val test = new TestClass
      val result = test.getExtension(".thing")

      result must beSome("thing")
    }

    "not break if there is an empty string" in {
      val test = new TestClass
      val result = test.getExtension("")

      result must beNone
    }
  }

  "FilenameHelpers.determineFileName" should {
    "take the filepath from the item, if present" in {
      val fakeItem = mock[VSLazyItem]
      fakeItem.getSingle(any) returns Some("/path/to/some/file.mxf")

      val fakeFile = mock[VSFile]
      fakeFile.path returns "/path/to/some/proxy.mp4"
      val fakeShape = mock[VSShape]
      fakeShape.files returns Seq(fakeFile)

      val test = new TestClass
      val result = test.determineFileName(fakeItem, Some(fakeShape))
      result must beSome("/path/to/some/file.mxf")
      there was one(fakeItem).getSingle("gnm_asset_filename")
    }

    "take the filepath from the shape, if not present on the item" in {
      val fakeItem = mock[VSLazyItem]
      fakeItem.getSingle(any) returns None

      val fakeFile = mock[VSFile]
      fakeFile.path returns "/path/to/some/proxy.mp4"
      val fakeShape = mock[VSShape]
      fakeShape.files returns Seq(fakeFile)

      val test = new TestClass
      val result = test.determineFileName(fakeItem, Some(fakeShape))
      result must beSome("/path/to/some/proxy.mp4")
      there was one(fakeItem).getSingle("gnm_asset_filename")
    }

    "return None if no filepath can be determined" in {
      val fakeItem = mock[VSLazyItem]
      fakeItem.getSingle(any) returns None


      val fakeShape = mock[VSShape]
      fakeShape.files returns Seq()

      val test = new TestClass
      val result = test.determineFileName(fakeItem, Some(fakeShape))
      result must beNone
      there was one(fakeItem).getSingle("gnm_asset_filename")
    }
  }

  "FilenameHelpers.fixFileExtension" should {
    "apply the VSFile's extension to the given name" in {
      val fakeFile = mock[VSFile]
      fakeFile.path returns "/path/to/some/proxy.mp4"

      val test = new TestClass
      val result = test.fixFileExtension("/path/to/some/file.mxf", fakeFile)
      result mustEqual "/path/to/some/file.mp4"
    }

    "remove the extension if the VSFile does not have one" in {
      val fakeFile = mock[VSFile]
      fakeFile.path returns "/path/to/some/proxy"

      val test = new TestClass
      val result = test.fixFileExtension("/path/to/some/file.mxf", fakeFile)
      result mustEqual "/path/to/some/file"
    }

    "append the extension if the original name does not have one" in {
      val fakeFile = mock[VSFile]
      fakeFile.path returns "/path/to/some/proxy.mp4"

      val test = new TestClass
      val result = test.fixFileExtension("/path/to/some/file", fakeFile)
      result mustEqual "/path/to/some/file.mp4"
    }
  }
}

package streamcomponents

import org.specs2.mutable.Specification
import java.nio.file.Paths

class FilenameHelpersSpec extends Specification {
  import FilenameHelpers._

  "FilenameHelpers.PathXtn" should {
    "extract filename and extension into seperate properties" in {
      val result = PathXtn(Paths.get("some/path/to/some/file.xtn"))
      result.path mustEqual "some/path/to/some/file"
      result.xtn must beSome("xtn")
      result.hasExtension must beTrue
    }

    "handle a case with no file extension" in {
      val result = PathXtn(Paths.get("some/path/to/some/file"))
      result.path mustEqual "some/path/to/some/file"
      result.xtn must beNone
      result.hasExtension must beFalse
    }

    "handle multiple dots in the filename" in {
      val result = PathXtn(Paths.get("some/path/to/some/file.with.stuff.xtn"))
      result.path mustEqual "some/path/to/some/file.with.stuff"
      result.xtn must beSome("xtn")
      result.hasExtension must beTrue
    }
  }
}

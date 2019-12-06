package streamComponents

import org.specs2.mutable.Specification

class FilenameHelpersSpec extends Specification {
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
  }
}

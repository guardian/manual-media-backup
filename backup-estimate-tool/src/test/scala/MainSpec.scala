import java.io.File

import models.{BackupDebugInfo, FinalEstimate}
import org.specs2.mutable.Specification

import scala.io.Source

class MainSpec extends Specification {
  "Main.writeUnbackedupFiles" should {
    "write a single NDJSON file containing the information" in {
      val testData = FinalEstimate(0,0,0,0,Seq(
        BackupDebugInfo("/path/to/file1",None),
        BackupDebugInfo("/path/to/file2.ext",Some("some notes"))
      ))

      val result = Main.writeUnbackedupFiles(testData)
      result must beSuccessfulTry

      val outputFile = new File(s"${sys.env.getOrElse("HOME","/tmp")}/to-back-up.lst")
      val content = Source.fromFile(outputFile).mkString
      content must contain("{\"filePath\":\"/path/to/file1\",\"notes\":null}\n")
      content must contain("{\"filePath\":\"/path/to/file2.ext\",\"notes\":\"some notes\"}\n")
      content.split("\n").length mustEqual 2
    }
  }
}

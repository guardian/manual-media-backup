package helpers

import java.io.File

import org.specs2.mutable.Specification

class UnixStatSpec extends Specification {
  "anotherGetStatInfo" should {
    "work" in {
      val f = new File("README.md")
      val result = UnixStat.anotherGetStatInfo(f.toPath)
      println(result)
      1 mustEqual 0
    }
  }
}

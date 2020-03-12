package streamcomponents

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.slf4j.LoggerFactory
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

class ArchiveHunterExistsSpec extends Specification with Mockito {
  "stripPath" should {
    "remove the given number of parts from the provided string" in {
      implicit val system = mock[ActorSystem]
      implicit val mat:Materializer = mock[Materializer]
      implicit val logger = LoggerFactory.getLogger(getClass)

      val toTest = new ArchiveHunterExists("","",5)

      val result = toTest.stripPath("/path/to/some/long/filename/that/goes/on")
      result mustEqual("filename/that/goes/on")
    }
  }
}

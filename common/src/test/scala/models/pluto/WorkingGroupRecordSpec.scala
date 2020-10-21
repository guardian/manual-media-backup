package models.pluto

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import helpers.PlutoCommunicatorFuncs
import org.specs2.mutable.Specification
import io.circe.generic.auto._
import org.slf4j.LoggerFactory
import org.specs2.mock.Mockito
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class WorkingGroupRecordSpec extends Specification with Mockito{
  "WorkingGroupRecord" should {
    "be automatically parseable from live environment data" in {
      val testContent = """{
                          |    "status": "ok",
                          |    "count": 1,
                          |    "result": [
                          |        {
                          |            "id": 1,
                          |            "hide": false,
                          |            "name": "Multimedia Anti-Social",
                          |            "commissioner": "Boyd Paul"
                          |        }
                          |    ]
                          |}""".stripMargin
      class TestPlutoCommFuncs extends PlutoCommunicatorFuncs {
        override val plutoSharedSecret:String = "secret"
        override val plutoBaseUri: String = "http://localhost"
        override val logger = LoggerFactory.getLogger(getClass)
        override val mat:Materializer = mock[Materializer]
        override val system = mock[ActorSystem]
      }

      val toTest = new TestPlutoCommFuncs
      val result = Await.result(toTest.contentBodyToJson[Seq[WorkingGroupRecord]](Future(testContent)), 30 seconds)

      result must beSome(Seq(WorkingGroupRecord(
        1,
        "Multimedia Anti-Social",
        Some(false),
        "Boyd Paul"
      )))
    }
  }
}

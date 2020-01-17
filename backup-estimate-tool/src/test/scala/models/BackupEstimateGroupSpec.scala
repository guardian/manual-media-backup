package models

import java.time.{ZoneId, ZonedDateTime}

import akka.actor.Props
import org.specs2.mutable
import akka.pattern.ask
import models.BackupEstimateGroup.BEMsg

import scala.concurrent.Await
import scala.concurrent.duration._

class BackupEstimateGroupSpec extends mutable.Specification {
  implicit val timeout:akka.util.Timeout = 1 second

  "BackupEstimateGroup ! AddToGroup" should {
    "store the given entry in its data record and return success" in new AkkaTestkitSpecs2Support {
      val toTest = system.actorOf(Props(classOf[BackupEstimateGroup]))

      val testData = BackupEstimateEntry("path/to/something",12345L, ZonedDateTime.of(2020,1,2,3,4,5,0,ZoneId.systemDefault()))
      val result = Await.result(toTest ? BackupEstimateGroup.AddToGroup(testData), 1 second)

      result mustEqual akka.actor.Status.Success
    }

  }

  "BackupEstimateGroup ! FindEntryFor" should {
    "retrieve an existing record" in new AkkaTestkitSpecs2Support {
      val toTest = system.actorOf(Props(classOf[BackupEstimateGroup]))

      val testData = BackupEstimateEntry("path/to/something",12345L, ZonedDateTime.of(2020,1,2,3,4,5,0,ZoneId.systemDefault()))
      Await.ready(toTest ? BackupEstimateGroup.AddToGroup(testData), 1 second)
      val testData2 = BackupEstimateEntry("path/to/something_else",12345L, ZonedDateTime.of(2020,1,2,3,4,5,0,ZoneId.systemDefault()))
      Await.ready(toTest ? BackupEstimateGroup.AddToGroup(testData2), 1 second)
      val testData3 = BackupEstimateEntry("path/to/something_more",12345L, ZonedDateTime.of(2020,1,2,3,4,5,0,ZoneId.systemDefault()))
      Await.ready(toTest ? BackupEstimateGroup.AddToGroup(testData3), 1 second)

      val result = Await.result((toTest ? BackupEstimateGroup.FindEntryFor("path/to/something")).mapTo[BEMsg], 1 second)
      result must beAnInstanceOf[BackupEstimateGroup.FoundEntry]

      result.asInstanceOf[BackupEstimateGroup.FoundEntry].entries.headOption must beSome(testData)
      result.asInstanceOf[BackupEstimateGroup.FoundEntry].entries.length mustEqual 1
    }

    "respond NotFound if the entry is not found" in new AkkaTestkitSpecs2Support {
      val toTest = system.actorOf(Props(classOf[BackupEstimateGroup]))

      val testData = BackupEstimateEntry("path/to/something",12345L, ZonedDateTime.of(2020,1,2,3,4,5,0,ZoneId.systemDefault()))
      Await.ready(toTest ? BackupEstimateGroup.AddToGroup(testData), 1 second)
      val testData2 = BackupEstimateEntry("path/to/something_else",12345L, ZonedDateTime.of(2020,1,2,3,4,5,0,ZoneId.systemDefault()))
      Await.ready(toTest ? BackupEstimateGroup.AddToGroup(testData2), 1 second)
      val testData3 = BackupEstimateEntry("path/to/something_more",12345L, ZonedDateTime.of(2020,1,2,3,4,5,0,ZoneId.systemDefault()))
      Await.ready(toTest ? BackupEstimateGroup.AddToGroup(testData3), 1 second)

      val result = Await.result((toTest ? BackupEstimateGroup.FindEntryFor("gdsfsdfgdfgdsfsdgsd")).mapTo[BEMsg], 1 second)
      result mustEqual BackupEstimateGroup.NotFoundEntry
    }
  }
}

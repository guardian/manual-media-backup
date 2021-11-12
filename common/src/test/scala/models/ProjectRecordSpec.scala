package models

import java.time.{ZoneId, ZonedDateTime}
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import helpers.{LocalDateTimeEncoder, PlutoCommunicatorFuncs}
import org.specs2.mutable.Specification
import io.circe.generic.auto._
import io.circe.syntax._
import models.pluto.ProjectRecord
import org.slf4j.LoggerFactory
import org.specs2.mock.Mockito

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class ProjectRecordSpec extends Specification with LocalDateTimeEncoder with Mockito {
  "ProjectRecord" should {
    "be readable from a json blob" in {
      val jsonSource = """{
                         |    "status": "ok",
                         |    "result": {
                         |        "id": 1,
                         |        "projectTypeId": 1,
                         |        "vidispineId": "VX-1234",
                         |        "title": "hgdhfhjhgjgff",
                         |        "created": "2020-09-03T10:04:40.519+0000",
                         |        "updated": "2020-09-14T11:49:37.393+0000",
                         |        "user": "72adedb0-f332-488c-b393-d8482b229d8a",
                         |        "workingGroupId": 1,
                         |        "commissionId": 1,
                         |        "deletable": true,
                         |        "deep_archive": false,
                         |        "sensitive": false,
                         |        "status": "New",
                         |        "productionOffice": "UK"
                         |    }
                         |}""".stripMargin
      class TestPlutoCommFuncs extends PlutoCommunicatorFuncs {
        override val plutoSharedSecret:String = "secret"
        override val plutoBaseUri: String = "http://localhost"
        override val logger = LoggerFactory.getLogger(getClass)
        override val mat:Materializer = mock[Materializer]
        override val system = mock[ActorSystem]
      }

      val toTest = new TestPlutoCommFuncs
      val result = Await.result(toTest.contentBodyToJson[ProjectRecord](Future(jsonSource)), 5 seconds)
      result must beSome(ProjectRecord(
        1,
        1,
        Some("VX-1234"),
        "hgdhfhjhgjgff",
        ZonedDateTime.of(2020,9,3,10,4,40,519000000,ZoneId.of("Z")),
        ZonedDateTime.of(2020,9,14,11,49,37,393000000,ZoneId.of("Z")),
        "72adedb0-f332-488c-b393-d8482b229d8a",
        Some(1),
        Some(1),
        Some(true),
        Some(false),
        Some(false),
        "New",
        "UK"
      ))
    }

    "parse Z-terminated date strings" in {
      val jsonSource = """{
                         |    "status": "ok",
                         |    "result": {
                         |        "id": 1,
                         |        "projectTypeId": 1,
                         |        "vidispineId": "VX-1234",
                         |        "title": "hgdhfhjhgjgff",
                         |        "created": "2021-10-25T16:07:26.442Z",
                         |        "updated": "2021-10-25T16:07:26.442Z",
                         |        "user": "72adedb0-f332-488c-b393-d8482b229d8a",
                         |        "workingGroupId": 1,
                         |        "commissionId": 1,
                         |        "deletable": true,
                         |        "deep_archive": false,
                         |        "sensitive": false,
                         |        "status": "New",
                         |        "productionOffice": "UK"
                         |    }
                         |}""".stripMargin
      class TestPlutoCommFuncs extends PlutoCommunicatorFuncs {
        override val plutoSharedSecret:String = "secret"
        override val plutoBaseUri: String = "http://localhost"
        override val logger = LoggerFactory.getLogger(getClass)
        override val mat:Materializer = mock[Materializer]
        override val system = mock[ActorSystem]
      }

      val toTest = new TestPlutoCommFuncs
      val result = Await.result(toTest.contentBodyToJson[ProjectRecord](Future(jsonSource)), 5 seconds)
      result must beSome(ProjectRecord(
        1,
        1,
        Some("VX-1234"),
        "hgdhfhjhgjgff",
        ZonedDateTime.of(2021,10,25,16,7,26,442000000,ZoneId.of("Z")),
        ZonedDateTime.of(2021,10,25,16,7,26,442000000,ZoneId.of("Z")),
        "72adedb0-f332-488c-b393-d8482b229d8a",
        Some(1),
        Some(1),
        Some(true),
        Some(false),
        Some(false),
        "New",
        "UK"
      ))
    }

    "parse microsecond timestamps" in {
      val jsonSource = """{
                         |    "status": "ok",
                         |    "result": {
                         |        "id": 1,
                         |        "projectTypeId": 1,
                         |        "vidispineId": "VX-1234",
                         |        "title": "hgdhfhjhgjgff",
                         |        "created": "2021-03-23T16:52:32.567922Z",
                         |        "updated": "2021-03-23T16:52:32.567922Z",
                         |        "user": "72adedb0-f332-488c-b393-d8482b229d8a",
                         |        "workingGroupId": 1,
                         |        "commissionId": 1,
                         |        "deletable": true,
                         |        "deep_archive": false,
                         |        "sensitive": false,
                         |        "status": "New",
                         |        "productionOffice": "UK"
                         |    }
                         |}""".stripMargin
      class TestPlutoCommFuncs extends PlutoCommunicatorFuncs {
        override val plutoSharedSecret:String = "secret"
        override val plutoBaseUri: String = "http://localhost"
        override val logger = LoggerFactory.getLogger(getClass)
        override val mat:Materializer = mock[Materializer]
        override val system = mock[ActorSystem]
      }

      val toTest = new TestPlutoCommFuncs
      val result = Await.result(toTest.contentBodyToJson[ProjectRecord](Future(jsonSource)), 5 seconds)
      result must beSome(ProjectRecord(
        1,
        1,
        Some("VX-1234"),
        "hgdhfhjhgjgff",
        ZonedDateTime.of(2021,3,23,16,52,32,567922000,ZoneId.of("Z")),
        ZonedDateTime.of(2021,3,23,16,52,32,567922000,ZoneId.of("Z")),
        "72adedb0-f332-488c-b393-d8482b229d8a",
        Some(1),
        Some(1),
        Some(true),
        Some(false),
        Some(false),
        "New",
        "UK"
      ))
    }

    "parse no-subsecond timestamps" in {
      val jsonSource = """{
                         |    "status": "ok",
                         |    "result": {
                         |        "id": 1,
                         |        "projectTypeId": 1,
                         |        "vidispineId": "VX-1234",
                         |        "title": "hgdhfhjhgjgff",
                         |        "created": "2021-04-29T23:00:00Z",
                         |        "updated": "2021-04-29T23:00:00Z",
                         |        "user": "72adedb0-f332-488c-b393-d8482b229d8a",
                         |        "workingGroupId": 1,
                         |        "commissionId": 1,
                         |        "deletable": true,
                         |        "deep_archive": false,
                         |        "sensitive": false,
                         |        "status": "New",
                         |        "productionOffice": "UK"
                         |    }
                         |}""".stripMargin
      class TestPlutoCommFuncs extends PlutoCommunicatorFuncs {
        override val plutoSharedSecret:String = "secret"
        override val plutoBaseUri: String = "http://localhost"
        override val logger = LoggerFactory.getLogger(getClass)
        override val mat:Materializer = mock[Materializer]
        override val system = mock[ActorSystem]
      }

      val toTest = new TestPlutoCommFuncs
      val result = Await.result(toTest.contentBodyToJson[ProjectRecord](Future(jsonSource)), 5 seconds)
      result must beSome(ProjectRecord(
        1,
        1,
        Some("VX-1234"),
        "hgdhfhjhgjgff",
        ZonedDateTime.of(2021,4,29,23,0,0,0,ZoneId.of("Z")),
        ZonedDateTime.of(2021,4,29,23,0,0,0,ZoneId.of("Z")),
        "72adedb0-f332-488c-b393-d8482b229d8a",
        Some(1),
        Some(1),
        Some(true),
        Some(false),
        Some(false),
        "New",
        "UK"
      ))
    }
  }
}

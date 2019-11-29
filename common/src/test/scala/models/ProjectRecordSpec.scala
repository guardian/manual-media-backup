package models

import java.util.UUID

import helpers.LocalDateTimeEncoder
import org.specs2.mutable.Specification
import io.circe.generic.auto._
import io.circe.syntax._

class ProjectRecordSpec extends Specification with LocalDateTimeEncoder {
  "ProjectRecord" should {
    "be readable from a json blob" in {
      val jsonSource = """{
                         |  "collection_id": 13,
                         |  "user": 1,
                         |  "created": "2018-08-01T12:21:12.532",
                         |  "updated": "2019-02-26T10:21:53.518",
                         |  "commission": 10,
                         |  "gnm_project_status": "Held",
                         |  "gnm_project_standfirst": null,
                         |  "gnm_project_headline": "dasdsadsadsa test 2",
                         |  "gnm_project_username": [
                         |    1
                         |  ],
                         |  "gnm_project_project_file_item": null,
                         |  "gnm_project_prelude_file_item": null,
                         |  "gnm_project_type": "0470ff3b-603d-456d-8ea3-e391ddbe11ce",
                         |  "project_locker_id": null,
                         |  "project_locker_id_prelude": null
                         |}""".stripMargin
      val json = io.circe.parser.parse(jsonSource).right.get
      val result = json.as[ProjectRecord]
      result must beRight
      val finalObj = result.right.get
      finalObj.collection_id mustEqual 13
      finalObj.user mustEqual 1
      finalObj.commission mustEqual 10
      finalObj.gnm_project_status mustEqual "Held"
      finalObj.gnm_project_standfirst must beNone
      finalObj.gnm_project_type mustEqual UUID.fromString("0470ff3b-603d-456d-8ea3-e391ddbe11ce")
    }
  }
}

package models.pluto

import java.util.UUID
import org.specs2.mutable.Specification


class WorkingGroupRecordSpec extends Specification {
  import WorkingGroupRecordDecoder._
  "WorkingGroupRecord" should {
    "be automatically parseable from live environment data" in {
      val testContent = """[{"hide": "workinggroup_hide", "name": "Something", "uuid": "166ADAFC-AB2D-4AA1-8B21-170BFCF276B6"}, {"hide": "workinggroup_hide", "name": "Something else", "uuid": "AC41E286-9CDA-40D3-B4AC-E483C8E25906"}, {"hide": null, "name": "Third thing", "uuid": "814D152A-084E-43A4-A524-D376B36666CD"}]"""

      val result = io.circe.parser.parse(testContent).flatMap(_.as[Seq[WorkingGroupRecord]])
      result must beRight

      result.right.get.length mustEqual 3
      result.right.get.head mustEqual WorkingGroupRecord(UUID.fromString("166ADAFC-AB2D-4AA1-8B21-170BFCF276B6"),"Something",Some(true))
      result.right.get(1) mustEqual WorkingGroupRecord(UUID.fromString("AC41E286-9CDA-40D3-B4AC-E483C8E25906"),"Something else",Some(true))
      result.right.get(2) mustEqual WorkingGroupRecord(UUID.fromString("814D152A-084E-43A4-A524-D376B36666CD"),"Third thing",None)

    }
  }
}

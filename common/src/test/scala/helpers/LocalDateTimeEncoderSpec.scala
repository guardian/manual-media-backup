package helpers

import io.circe.{HCursor, Json}
import org.specs2.mutable.Specification

import java.time.ZoneOffset

class LocalDateTimeEncoderSpec extends Specification with LocalDateTimeEncoder {
  "decodeZonedDateTime" should {
    "support basic format" in {
      val value = HCursor.fromJson(Json.fromString("2001-02-03T04:05:06Z"))
      val result = decodeZonedDateTime(value)
      result must beRight
      val output = result.right.get
      output.getYear mustEqual 2001
      output.getMonthValue mustEqual 2
      output.getDayOfMonth mustEqual 3
      output.getHour mustEqual 4
      output.getMinute mustEqual 5
      output.getSecond mustEqual 6
      output.getNano mustEqual 0
      output.getOffset mustEqual ZoneOffset.UTC
    }

    "support milliseconds" in {
      val value = HCursor.fromJson(Json.fromString("2001-02-03T04:05:06.789Z"))
      val result = decodeZonedDateTime(value)
      result must beRight
      val output = result.right.get
      output.getYear mustEqual 2001
      output.getMonthValue mustEqual 2
      output.getDayOfMonth mustEqual 3
      output.getHour mustEqual 4
      output.getMinute mustEqual 5
      output.getSecond mustEqual 6
      output.getNano mustEqual 789000000
      output.getOffset mustEqual ZoneOffset.UTC
    }

    "support partial milliseconds" in {
      val value = HCursor.fromJson(Json.fromString("2018-03-12T14:54:46.01Z"))
      val result = decodeZonedDateTime(value)
      result must beRight
      val output = result.right.get
      output.getYear mustEqual 2018
      output.getMonthValue mustEqual 3
      output.getDayOfMonth mustEqual 12
      output.getHour mustEqual 14
      output.getMinute mustEqual 54
      output.getSecond mustEqual 46
      output.getNano mustEqual 10000000
      output.getOffset mustEqual ZoneOffset.UTC
    }

    "support positive timezone offset" in {
      val value = HCursor.fromJson(Json.fromString("2001-02-03T04:05:06.789+05:30"))
      val result = decodeZonedDateTime(value)
      result must beRight
      val output = result.right.get
      output.getYear mustEqual 2001
      output.getMonthValue mustEqual 2
      output.getDayOfMonth mustEqual 3
      output.getHour mustEqual 4
      output.getMinute mustEqual 5
      output.getSecond mustEqual 6
      output.getNano mustEqual 789000000
      output.getOffset mustEqual ZoneOffset.ofHoursMinutes(5,30)
    }

    "support negative timezone offset" in {
      val value = HCursor.fromJson(Json.fromString("2001-02-03T04:05:06.789-03:00"))
      val result = decodeZonedDateTime(value)
      result must beRight
      val output = result.right.get
      output.getYear mustEqual 2001
      output.getMonthValue mustEqual 2
      output.getDayOfMonth mustEqual 3
      output.getHour mustEqual 4
      output.getMinute mustEqual 5
      output.getSecond mustEqual 6
      output.getNano mustEqual 789000000
      output.getOffset mustEqual ZoneOffset.ofHours(-3)
    }
  }

}

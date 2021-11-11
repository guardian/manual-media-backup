package helpers

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}

trait LocalDateTimeEncoder {
  //this string will match a bare datetime, a bare datetime with milliseconds [.SSS], zone-offsets +0000 or numbers [Z] or zone offset Z or numbers [X]
  protected val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS][Z][X]")

  implicit val encodeLocalDateTime: Encoder[LocalDateTime] = (a: LocalDateTime) => Json.fromString(a.format(dateTimeFormatter))

  implicit val decodeLocalDateTime: Decoder[LocalDateTime] = (c: HCursor) => for {
    str <- c.value.as[String]
  } yield LocalDateTime.parse(str, dateTimeFormatter)

  implicit val encodeZonedDateTime: Encoder[ZonedDateTime] = (a: ZonedDateTime) => Json.fromString(a.format(dateTimeFormatter))

  implicit val decodeZonedDateTime: Decoder[ZonedDateTime] = (c: HCursor) => for {
    str <- c.value.as[String]
  } yield ZonedDateTime.parse(str, dateTimeFormatter)
}

//also make it available as an importable object, as i have been having issues with retry-recursion when
//simply extending the trait
object LocalDateTimeEncoder extends LocalDateTimeEncoder
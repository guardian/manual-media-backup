package helpers

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}

trait LocalDateTimeEncoder {
  protected val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")

  implicit val encodeZonedDateTime: Encoder[LocalDateTime] = (a: LocalDateTime) => Json.fromString(a.format(dateTimeFormatter))

  implicit val decodeZonedDateTime: Decoder[LocalDateTime] = (c: HCursor) => for {
    str <- c.value.as[String]
  } yield LocalDateTime.parse(str, dateTimeFormatter)
}

//also make it available as an importable object, as i have been having issues with retry-recursion when
//simply extending the trait
object LocalDateTimeEncoder extends LocalDateTimeEncoder
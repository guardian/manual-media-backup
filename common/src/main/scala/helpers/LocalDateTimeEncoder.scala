package helpers

import auth.HMAC.logger

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}

import scala.util.{Failure, Success, Try}

trait LocalDateTimeEncoder {
  //this string will match a bare datetime, a bare datetime with milliseconds [.SSS], zone-offsets +0000 or numbers [Z] or zone offset Z or numbers [X]
  protected val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS[Z][X]")
  //also support microseconds
  protected val secondaryDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][Z][X]")
  implicit val encodeLocalDateTime: Encoder[LocalDateTime] = (a: LocalDateTime) => Json.fromString(a.format(dateTimeFormatter))

  implicit val decodeLocalDateTime: Decoder[LocalDateTime] = (c: HCursor) => for {
    str <- c.value.as[String]
  } yield parseLDT(str)

  implicit val encodeZonedDateTime: Encoder[ZonedDateTime] = (a: ZonedDateTime) => Json.fromString(a.format(dateTimeFormatter))

  implicit val decodeZonedDateTime: Decoder[ZonedDateTime] = (c: HCursor) => for {
    str <- c.value.as[String]
  } yield parseZDT(str)

  /**
    * tries to parse the given string with `dateTimeFormatter` and if that fails falls back to `secondaryDateTimeFormatter`.
    * Throws an exception on failure, as this is the behaviour expected
    * @param string date string to parse
    * @return ZonedDateTime representing the time
    */
  def parseZDT(string:String):ZonedDateTime = {
    Try {
      ZonedDateTime.parse(string, dateTimeFormatter)
    } match {
      case Success(dt)=>dt
      case Failure(err)=>
        logger.debug(s"Could not parse $string with ${dateTimeFormatter.toString}: ${err.getMessage}, trying backup")
        ZonedDateTime.parse(string, secondaryDateTimeFormatter)
    }
  }

  /**
    * tries to parse the given string with `dateTimeFormatter` and if that fails falls back to `secondaryDateTimeFormatter`.
    * Throws an exception on failure, as this is the behaviour expected
    * @param string date string to parse
    * @return LocalDateTime representing the time
    */
  def parseLDT(string:String):LocalDateTime = {
    Try {
      LocalDateTime.parse(string, dateTimeFormatter)
    } match {
      case Success(dt)=>dt
      case Failure(err)=>
        logger.debug(s"Could not parse $string with ${dateTimeFormatter.toString}: ${err.getMessage}, trying backup")
        LocalDateTime.parse(string, secondaryDateTimeFormatter)
    }
  }
}

//also make it available as an importable object, as i have been having issues with retry-recursion when
//simply extending the trait
object LocalDateTimeEncoder extends LocalDateTimeEncoder
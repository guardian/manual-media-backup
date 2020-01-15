package models.pluto

import java.util.UUID

import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor}

case class WorkingGroupRecord(uuid:UUID, name:String, hide:Option[Boolean])

/**
  * You need to use this custom decoder if decoding via Circe, to convert the null-or-string logic
  * of the "hide" field to an Option[Boolean]
  */
object WorkingGroupRecordDecoder {
  implicit val decodeWorkingGroupRecord: Decoder[WorkingGroupRecord] = new Decoder[WorkingGroupRecord] {
    override def apply(c: HCursor): Result[WorkingGroupRecord] =
      for {
        uuid <- c.downField("uuid").as[UUID]
        name <- c.downField("name").as[String]
        maybeHideString <- c.downField("hide").as[Option[String]]
      } yield WorkingGroupRecord(uuid, name, maybeHideString.map(hideString=>hideString == "workinggroup_hide"))
  }
}
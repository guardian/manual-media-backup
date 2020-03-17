package vidispine

import java.time.ZonedDateTime
import java.util.UUID

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

case class VSMetadataValue(value:String, uuid:Option[UUID], user:Option[String], timestamp:Option[ZonedDateTime], change:Option[String])
case class VSMetadataEntry(name:String, uuid:Option[UUID], user:Option[String], timestamp:Option[ZonedDateTime], change:Option[String], values:Seq[VSMetadataValue])

object VSMetadataEntry {
  def safeUuidString(str:String,xml:NodeSeq):Option[UUID] = Try {
    UUID.fromString(str)
  } match {
    case Success(uuid)=>Some(uuid)
    case Failure(err)=>None
  }

  /**
    * same as Option(str), which returns None if str is null, but also treats an empty string "" as None
    * @param str string to test
    * @return an Option which is None if str is null or empty
    */
  def blankAsOption(str:String):Option[String] = Option(str).flatMap(actualStr=>
    if(actualStr=="") None else Some(actualStr)
  )

  /**
    * create a sequence of VSMetadataEntry objects from the provided parsed XML.
    * @param xml NodeSeq pointing to a "timespan" level of aVidispine MetadataDocument
    * @return a sequence of VSMetadataEntry objects
    */
  def fromXml(xml:NodeSeq):Seq[VSMetadataEntry] = {
    ((xml \ "field") ++ (xml \ "group" \ "field")).map(fieldNode=>
      new VSMetadataEntry((fieldNode \ "name").text,
        safeUuidString(fieldNode \@ "uuid",fieldNode),
        blankAsOption(fieldNode \@ "user"),
        blankAsOption(fieldNode \@ "timestamp").map(ZonedDateTime.parse),
        blankAsOption(fieldNode \@ "change"),
        (fieldNode \ "value").map(valueNode=>
          VSMetadataValue(
            valueNode.text,
            safeUuidString(valueNode \@ "uuid", valueNode),
            blankAsOption(valueNode \@ "user"),
            blankAsOption(fieldNode \@ "timestamp").map(ZonedDateTime.parse),
            blankAsOption(valueNode \@ "change")
          )
        )
      )
    )
  }

  /**
    * predominantly for testing, create a simple key->value with current time and a generated UUID
    * @param key
    * @param value
    */
  def simple(key:String, value:String) = {
    new VSMetadataEntry(key,
      Some(UUID.randomUUID()),
    Some("system"),
      Some(ZonedDateTime.now()),
      None,
      Seq(VSMetadataValue(value,Some(UUID.randomUUID()),Some("system"),Some(ZonedDateTime.now()),None)))
  }
}
package vidispine

import java.time.ZonedDateTime

import com.softwaremill.sttp._

import scala.util.Try
import scala.xml.NodeSeq

case class VSStorageMethod (vsid:String, uri:Uri, read:Boolean, write: Boolean, browse: Boolean, lastSuccess:Option[ZonedDateTime], methodType:String)

object VSStorageMethod extends ((String,Uri,Boolean,Boolean,Boolean,Option[ZonedDateTime], String)=>VSStorageMethod) {
  def stringToBool(str:String):Boolean = {
    str match {
      case "false"=>false
      case "no"=>false
      case "0"=>false
      case "true"=>true
      case "yes"=>true
      case "1"=>true
      case _=>throw new RuntimeException(s"Could not convert $str to boolean")
    }
  }

  /**
    * returns an option which is None if the incoming string is either Null or zero-length
    * @param maybeStr - maybe a valid string
    * @return an Option[String]
    */
  def optionWithNullEmpty(maybeStr:String):Option[String] = maybeStr match {
    case null=>None
    case ""=>None
    case _=>Some(maybeStr)
  }

  def fromXml(node:NodeSeq):Try[VSStorageMethod] = Try {
    new VSStorageMethod(
      (node \ "id").text,
      uri"${(node \ "uri").text}",
      stringToBool((node \ "read").text),
      stringToBool((node \ "write").text),
      stringToBool((node \ "browse").text),
      Option(node \ "lastSuccess").flatMap(n=>optionWithNullEmpty(n.text)).map(str=>ZonedDateTime.parse(str)),
      (node \ "type").text
    )
  }
}
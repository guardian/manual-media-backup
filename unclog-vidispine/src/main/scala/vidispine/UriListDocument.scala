package vidispine

import scala.util.Try
import scala.xml.{NodeSeq, XML}

/**
  * this class represents the parsed out content of a Vidispine UriListDocument
  * @param uri
  */
case class UriListDocument (uri:Seq[String])

object UriListDocument {
  def fromXmlString(xmlString:String) = fromXml(XML.loadString(xmlString))

  def fromXml(elem: NodeSeq):Try[UriListDocument] = Try {
    new UriListDocument(
      (elem \ "uri").map(uriElem=>uriElem.text)
    )
  }
}
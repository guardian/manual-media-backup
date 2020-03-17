package vidispine

import org.slf4j.LoggerFactory

import scala.xml.{Node, XML}

/**
  * helper class that represents a generic error from the Vidispine API.
  * the `fromXml` methods on the companion object initialise this from an XML string that is returned by the API.
  * Specific errors are represented by subclasses of this class.
  * @param errorMessage
  */
class VSError(errorMessage:String) extends Throwable {
  override def getMessage: String = errorMessage

  override def toString: String = getMessage
}

object VSError {
  private val logger = LoggerFactory.getLogger(getClass)

  def fromXml(xmlText: String):Either[String,VSError] = {
    try {
      logger.info(s"raw xml: $xmlText")
      fromXml(XML.loadString(xmlText))
    } catch {
      case except:Exception=>Left(xmlText)
    }
  }

  def fromXml(xmlElem: Node):Either[String,VSError] = {
    val exceptType = xmlElem.child.head.label
    if(exceptType=="invalidInput") {
      Right(VSErrorInvalidInput(exceptType, (xmlElem \\ "context").text, (xmlElem \\ "explanation").text))
    } else if(exceptType=="notFound") {
      Right(VSErrorNotFound(exceptType, (xmlElem \\ "type").text, (xmlElem \\ "id").text))
    } else {
      Left(xmlElem.toString())
    }
  }
}

case class VSErrorNotFound(exceptType: String, entityType: String, entityId: String) extends VSError(exceptType){
  override def getMessage: String = s"$exceptType: $entityType $entityId"
}

case class VSErrorInvalidInput(exceptType: String, context: String, explanation:String) extends VSError(exceptType){
  override def getMessage: String = s"$exceptType: $explanation (context is $context)"

}

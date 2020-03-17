package vidispine

import akka.stream.Materializer
import models.HttpError
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.xml.{Node, XML}

case class GetMetadataError(httpError:Option[HttpError], vsError:Option[VSError], xmlError:Option[String])

case class VSLazyItem (itemId:String, lookedUpMetadata:Map[String,VSMetadataEntry]=Map(), shapes:Option[Map[String,VSShape]]=None) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * looks up the given field list and returns a new object with their values set in `lookedUpMetadata`.
    * since this goes over the network, it is a future and may fail; a Left is returned in this case.
    * @param fieldList metadata fields to look up
    * @param comm implicitly provided VSCommunicator object
    * @param mat implicitly provided akka Materializer
    * @return
    */
  def getMoreMetadata(fieldList:Seq[String])(implicit comm:VSCommunicator, mat:Materializer):Future[Either[GetMetadataError,VSLazyItem]] =
    comm.requestGet(s"/API/item/$itemId/metadata?field=${fieldList.mkString(",")}", Map("Accept"->"application/xml")).map({
      case Right(returnedXml) =>
        val maybeParsedData = Try {
          XML.loadString(returnedXml)
        }
        maybeParsedData match {
          case Success(parsedData) =>
            val newMetadataSeq = VSMetadataEntry.fromXml(parsedData \ "item" \ "metadata" \ "timespan")
            logger.debug(s"Got ${newMetadataSeq.length} more metadata keys")

            val newMetadataMap = newMetadataSeq.map(entry => entry.name -> entry).toMap
            Right(this.copy(lookedUpMetadata = this.lookedUpMetadata ++ newMetadataMap))
          case Failure(parseError) =>
            logger.error(s"Could not parse XML returned from Vidispine: ", parseError)
            Left(GetMetadataError(None, None, Some(parseError.toString)))
        }
      case Left(httpErr) =>
        Left(GetMetadataError(Some(httpErr), None, None))
    })

  /**
    * get any metadata for the given key
    */
  def get(fieldName:String):Option[Seq[String]] = {
    lookedUpMetadata.get(fieldName).map(_.values.map(_.value))
  }

  def getSingle(fieldName:String):Option[String] = {
    get(fieldName).flatMap(_.headOption)
  }
}

object VSLazyItem extends ((String,Map[String,VSMetadataEntry],Option[Map[String,VSShape]])=>VSLazyItem) {
  /**
    * simple constructor for blank item
    * @param itemId item id to hold
    * @return VSLazyItem with nothing loaded
    */
  def apply(itemId:String) = new VSLazyItem(itemId,Map(),None)

  /**
    * construct from a single <item> stanza in an ItemListDocument
    * @param xml parsed xml node pointing to an <item>
    */
  def fromXmlSearchStanza(xml:Node) = {
    val itemId = xml \@ "id"

    val metadataSeq = VSMetadataEntry.fromXml(xml \ "metadata" \ "timespan")
    val newMetadataMap = metadataSeq.map(entry => entry.name -> entry).toMap
    val shapes = (xml \ "shape").map(VSShape.fromXml)
    val shapesMap = shapes.map(entry=>entry.tag->entry).toMap

    new VSLazyItem(itemId,newMetadataMap,if(shapesMap.isEmpty) None else Some(shapesMap))
  }
}
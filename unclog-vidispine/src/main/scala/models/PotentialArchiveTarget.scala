package models

import io.circe._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

object ArchiveTargetStatus extends Enumeration {
  val IN_PROGRESS,TARGET_CONFLICT, UPLOAD_FAILED, DELETE_FAILED, SUCCESS = Value

}

case class PotentialArchiveTarget(byteSize:Option[Long], md5Hash:Option[String], archivedSize:Option[Long], oid:String, mxsFilename:String,
                                  vsFileId:String, vsItemAttachment:Option[String],
                                  vsShapeAttachment:Option[Seq[String]], status:ArchiveTargetStatus.Value)

object PotentialArchiveTarget {
  private val logger = LoggerFactory.getLogger(getClass)

  def apply(byteSize: Option[Long], md5Hash: Option[String], oid: String, mxsFilename: String, vsFileId: String, vsItemAttachment: Option[String], vsShapeAttachment: Option[Seq[String]]): PotentialArchiveTarget =
    new PotentialArchiveTarget(byteSize, md5Hash, None, oid, mxsFilename, vsFileId, vsItemAttachment, vsShapeAttachment, ArchiveTargetStatus.IN_PROGRESS)

  /**
    * internal helper method that pulls a list of values out of some nested json
    * @param listCursor cursor at which to decode. This must point to an array, which contains objects with a "shapeId" key
    * @param currentValues Sequence of values to start with. Normally pass an empty sequence
    * @tparam A the data type that is to be extracted
    * @return
    */
  private def getList[A:io.circe.Decoder](listCursor:ACursor, currentValues:Seq[A]):Option[Seq[A]] = {
    listCursor.downField("shapeId").as[A] match {
      case Left(err)=>
        logger.warn(s"$err, assuming end of array")
        if(currentValues.isEmpty) {
          None
        } else {
          Some(currentValues)
        }
      case Right(value)=>
        getList(listCursor.right,currentValues :+ value)
    }
  }

  /**
    * manual decode from incoming json to a PotentialArchiveTarget instance
    * @param jsonString raw json string for parsing
    * @return either the PotentialArchiveTarget or an error
    */
  def fromMediaCensusJson(jsonString:String):Try[PotentialArchiveTarget] = {
    io.circe.parser.parse(jsonString) match {
      case Left(parseErr)=>
        logger.error(s"Offending data was: $jsonString")
        logger.error(s"Could not parse incoming json record: $parseErr")
        Failure(new RuntimeException("could not parse record"))
      case Right(json)=>
        val metadata = json.hcursor.downField("metadata")

        Try {
          PotentialArchiveTarget(
            json.hcursor.downField("size").as[Long].toOption,
            json.hcursor.downField("hash").as[String].toOption,
            metadata.downField("uuid").as[String].right.get,
            metadata.downField("MXFS_FILENAME").as[String].right.get,
            json.hcursor.downField("vsid").as[String].right.get,
            json.hcursor.downField("membership").downField("itemId").as[String].toOption,
            getList[String](json.hcursor.downField("membership").downField("shapes").downArray, Seq())
           // json.hcursor.downField("membership").downField("shapes").downArray.downField("shapeId").as[Seq[String]].toOption
          )
        }
    }
  }
}

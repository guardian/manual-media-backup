package streamComponents

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

case class LostFileEntry (fileId:String, shapeId:String, itemId:String, itemPath:Option[String], storageId:String, filePath:String, fileSize:Long, timestamp:ZonedDateTime) {
  def csvRow:String = {
    s""""$fileId","$shapeId","$itemId","${itemPath.getOrElse("[none]")}","$storageId","$filePath",$fileSize,"${timestamp.format(DateTimeFormatter.ISO_DATE_TIME)}"""" + "\n"
  }
}

object LostFileEntry {
  def headerRow:String={
    """"FileID","ShapeID","ItemId","Item path","StorageId","File path","Filesize","Timestamp"""" + "\n"
  }
}
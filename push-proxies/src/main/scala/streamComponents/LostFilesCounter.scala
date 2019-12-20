package streamComponents

import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets

import akka.actor.Actor
import com.gu.vidispineakka.vidispine.{VSFile, VSLazyItem, VSShape}
import org.slf4j.LoggerFactory

object LostFilesCounter {
  trait LFMsg

  /**
    * dispatched to register that no files are present for the given shape on the given item
    * @param vsShape
    * @param vsItem
    */
  case class RegisterLost(vsFile:VSFile, vsShape:VSShape,vsItem:VSLazyItem) extends LFMsg

  /**
    * dispatched to request that the actor dumps out the accumulated list to CSV file on the local filesystem
    * @param toFilePath filepath to write
    */
  case class Dump(toFilePath:String) extends LFMsg
}

class LostFilesCounter extends Actor {
  import LostFilesCounter._
  private val logger = LoggerFactory.getLogger(getClass)
  private var lostFiles:Seq[LostFileEntry] = Seq()

  override def receive: Receive = {
    case RegisterLost(vsFile, vsShape, vsItem)=>
      lostFiles = lostFiles :+ LostFileEntry(
        fileId=vsFile.vsid,
        shapeId = vsShape.vsid,
        itemId = vsItem.itemId,
        itemPath = vsItem.getSingle("gnm_original_filename"),
        storageId = vsFile.storage,
        filePath=vsFile.path,
        fileSize=vsFile.size,
        timestamp = vsFile.timestamp
      )
      logger.info(s"Lost file found, now tracking ${lostFiles.length} lost files")
      sender ! akka.actor.Status.Success

    case Dump(filePath)=>
      val f = new File(filePath)
      val stream = new FileOutputStream(f)
      try {
        stream.write(LostFileEntry.headerRow.getBytes(StandardCharsets.UTF_8))
        lostFiles.foreach(entry => stream.write(entry.csvRow.getBytes(StandardCharsets.UTF_8)))
        sender() ! akka.actor.Status.Success
      } catch {
        case err:Throwable=>
          logger.error(s"Could not write lost files content to file: ", err)
          sender() ! akka.actor.Status.Failure(err)
      } finally {
        stream.close()
      }
  }
}

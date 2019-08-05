package streamcomponents

import java.time.ZonedDateTime

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.{IncomingListEntry, ObjectMatrixEntry}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class OMMetaToIncomingList (log:Boolean=false,logFields:Seq[String]=Seq("MXFS_FILENAME","MXFS_MODIFICATION_TIME","DPSP_SIZE"))(implicit mat:Materializer, ec:ExecutionContext) extends GraphStage[FlowShape[ObjectMatrixEntry, IncomingListEntry]] {
  private val in:Inlet[ObjectMatrixEntry] = Inlet.create("OMMetaToIncomingList.in")
  private val out:Outlet[IncomingListEntry] = Outlet.create("OMMetaToIncomingList.out")

  override def shape: FlowShape[ObjectMatrixEntry, IncomingListEntry] = FlowShape.of(in, out)

  /*
        stringValues = Map(
        "MXFS_FILENAME_UPPER" -> path.getFileName.toString.toUpperCase,
        "MXFS_FILENAME"->path.getFileName.toString,
        "MXFS_PATH"->path.toString,
        "MXFS_USERNAME"->uid.toString, //stored as a string for compatibility. There seems to be no easy way to look up the numeric UID in java/scala
        "MXFS_MIMETYPE"->mimeType.getOrElse("application/octet-stream"),
        "MXFS_DESCRIPTION"->s"File ${path.getFileName.toString}",
        "MXFS_PARENTOID"->"",
        "MXFS_FILEEXT"->getFileExt(path.getFileName.toString).getOrElse("")
      ),
      boolValues = Map(
        "MXFS_INTRASH"->false,
      ),
      longValues = Map(
        "DPSP_SIZE"->file.length(),
        "MXFS_MODIFICATION_TIME"->fsAttrs.get("lastModifiedTime").map(_.asInstanceOf[FileTime].toMillis).getOrElse(0),
        "MXFS_CREATION_TIME"->fsAttrs.get("creationTime").map(_.asInstanceOf[FileTime].toMillis).getOrElse(0),
        "MXFS_ACCESS_TIME"->fsAttrs.get("lastAccessTime").map(_.asInstanceOf[FileTime].toMillis).getOrElse(0),
      ),
      intValues = Map(
        "MXFS_CREATIONDAY"->maybeCtime.map(ctime=>ctime.getDayOfMonth).getOrElse(0),
        "MXFS_COMPATIBLE"->1,
        "MXFS_CREATIONMONTH"->maybeCtime.map(_.getMonthValue).getOrElse(0),
        "MXFS_CREATIONYEAR"->maybeCtime.map(_.getYear).getOrElse(0),
        "MXFS_CATEGORY"->categoryForMimetype(mimeType)
      )
   */
  private val allKnownFields = Seq("MXFS_FILENAME_UPPER","MXFS_FILENAME","MXFS_PATH","MXFS_USERNAME","MXFS_MIMETYPE","MXFS_DESCRIPTION","MXFS_PARENTOID","MXFS_FILEEXT",
    "MXFS_INTRASH","DPSP_SIZE","MXFS_MODIFICATION_TIME","MXFS_CREATION_TIME","MXFS_ACCESS_TIME","MXFS_CREATIONDAY","MXFS_COMPATIBLE","MXFS_CREATIONMONTH","MXFS_CREATIONYEAR","MXFS_CATEGORY")

  def splitFilePath(completePath:String) = {
    val splitter = "^(.*)/([^/]+)$".r

    val splitter(path,name) = completePath
    (path, name)
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val successCb = createAsyncCallback[IncomingListEntry](entry=>{
          push(out,entry)
        })
        val failureCb = createAsyncCallback[Throwable](err=>failStage(err))

        val elem = grab(in)
        elem.getMetadata.onComplete({
          case Success(updatedEntry)=>
            val path,name = updatedEntry.stringAttribute("MXFS_FILENAME").getOrElse("unknown/unknown")
            val output = new IncomingListEntry(
              path,
              name,
              updatedEntry.timeAttribute("MXFS_MODIFICATION_TIME").getOrElse(ZonedDateTime.now()),
              updatedEntry.longAttribute("DPSP_SIZE").getOrElse(-1)
            )
            if(log){
              val fieldList = if(logFields.isEmpty){
                allKnownFields
              } else {
                logFields
              }

              val fieldValuesString = updatedEntry.attributes.map(_.dumpString(fieldList))
              logger.info(s"${updatedEntry.oid}: ${fieldValuesString.getOrElse("(no metadata)")}")
            }
            successCb.invoke(output)

          case Failure(err)=>
            logger.error("Could not update metadata: ", err)
            failureCb.invoke(err)
        })
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
    })
  }
}

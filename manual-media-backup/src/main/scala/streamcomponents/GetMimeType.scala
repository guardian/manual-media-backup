package streamcomponents

import java.nio.file.Files

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import eu.medsea.mimeutil.MimeUtil2
import models.{BackupEntry, MxsMetadata}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * tries to determine the MIME type for the given file and add it to the ObjectMatrix metadata.
  * this requires a valid OM entry in the incoming [[BackupEntry]]
  */
class GetMimeType extends GraphStage[FlowShape[BackupEntry, BackupEntry]] {
  private final val in:Inlet[BackupEntry] = Inlet.create("GetMimeType.in")
  private final val out:Outlet[BackupEntry] = Outlet.create("GetMimeType.out")

  override def shape: FlowShape[BackupEntry, BackupEntry] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    private val mimeUtil = new MimeUtil2
    mimeUtil.registerMimeDetector("eu.medsea.mimeutil.detector.MagicMimeMimeDetector")
    mimeUtil.registerMimeDetector("eu.medsea.mimeutil.detector.ExtensionMimeDetector")

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val mimeTypeString = Try { Option(MimeUtil2.getMostSpecificMimeType(mimeUtil.getMimeTypes(elem.originalPath.toFile)).toString) }

        if(elem.maybeObjectMatrixEntry.isEmpty){
          logger.error(s"Incoming entry had no object matrix entry, this is required for this operation")
          failStage(new RuntimeException("No objectmatrix entry to add MIME data to"))
          return
        }
        mimeTypeString match {
          case Success(maybeMimeTypeValue)=>
            logger.debug(s"MIME type for ${elem.originalPath} is $maybeMimeTypeValue")
            val omEntry = elem.maybeObjectMatrixEntry.get
            val attribs = omEntry.attributes.getOrElse(MxsMetadata.empty())
            val updatedAttribs = attribs.withString("MXFS_MIMETYPE", maybeMimeTypeValue.getOrElse("application/octet-stream"))
            val updatedEntry = omEntry.copy(attributes = Some(updatedAttribs))
            val updatedElem = elem.copy(maybeObjectMatrixEntry = Some(updatedEntry))
            push(out, updatedElem)
          case Failure(err)=>
            logger.error(s"Could not determine MIME type for ${elem.originalPath}: ", err)
            failStage(err)
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

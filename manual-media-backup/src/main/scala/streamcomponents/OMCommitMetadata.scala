package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.Attribute
import com.om.mxs.client.japi.{AttributeView, MatrixStore, ObjectTypedAttributeView, UserInfo, Vault}
import models.{BackupEntry, MxsMetadata}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class OMCommitMetadata(userInfo:UserInfo) extends GraphStage[FlowShape[BackupEntry, BackupEntry]] {
  private val in:Inlet[BackupEntry] = Inlet.create("OMCommitMetadata.in")
  private val out:Outlet[BackupEntry] = Outlet.create("OMCommitMetadata.out")

  override def shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    private var maybeVault:Option[Vault] = None

    /**
      * write each attribute in turn; if any error then retry just those after a short delay
      * @param toWrite sequence of Attribute to write
      * @param attribView an AttributeView to write them to
      * @param attempt attempt number, don't set this when calling
      * @return either a Left with a sequence of each failed attribute and the corresponding error or a Right with the number
      *         of attributes set
      */
    def writeWithRetries(toWrite:Seq[Attribute], attribView:ObjectTypedAttributeView, attempt:Int=1):Either[Seq[(Attribute,Throwable)],Int] = {
      val writeTries = toWrite.map(attr=>(attr, Try { attribView.writeAttribute(attr) }))
      val writeFailures = writeTries.collect({case (failedAttr, Failure(err))=>(failedAttr, err)})
      val writeSuccesses = writeTries.collect({case(_, Success(x))=>x})
      toWrite.foreach(attrib=>{
        logger.debug(s"\tWriting ${attrib.getKey} = ${attrib.getValue} (${attrib.getValue.getClass.toGenericString}")
      })
      if(writeFailures.nonEmpty) {
        if(attempt<10){
          logger.error(s"Could not write attributes to file: ${writeSuccesses.length} succeeded and ${writeFailures.length} failed")
          writeFailures.foreach(err=>logger.error(s"\tFailed to write: ${err._1.getKey}: ${err._1.getValue.toString} of type ${Option(err._1.getValue).map(_.getClass.getCanonicalName)}"))
          Thread.sleep(500)
          writeWithRetries(writeFailures.map(_._1), attribView, attempt+1)
        } else {
          Left(writeFailures)
        }
      } else {
        Right(writeSuccesses.length)
      }
    }

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        elem.maybeObjectMatrixEntry match {
          case Some(omEntry)=>
            try {
              omEntry.attributes match {
                case Some(attribs)=>
                  val fileToWrite = maybeVault.get.getObject(omEntry.oid)
                  val attribView = fileToWrite.getAttributeView

                  writeWithRetries(attribs.toAttributes(filterUnwritable = true), attribView) match {
                    case Left(errorSeq)=>
                      failStage(new RuntimeException(s"Could not write ${errorSeq.length} attributes after 10 tries, giving up"))
                    case Right(count)=>
                      logger.info(s"Successfully wrote $count attributes")
                  }

                case None=>
                  logger.error(s"Incoming entry for ${omEntry.oid} had no metadata to write")
                  //not a fatal error
              }
              val updatedElem = elem.copy(maybeObjectMatrixEntry = Some(omEntry))
              push(out, updatedElem)
            } catch {
              case err:Throwable=>
                logger.error(s"Could not update metadata for ${omEntry.oid}: ", err)
                failStage(err)
            }
          case None=>
            logger.error(s"No objectmatrix entry present to write!")
            failStage(new RuntimeException("No objectmatrix entry to write"))
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })

    override def preStart(): Unit = {
      try {
        maybeVault = Some(MatrixStore.openVault(userInfo))
      } catch {
        case err:Throwable=>
          logger.error("Could not connect to vault: ", err)
          failStage(err)
      }
    }

    override def postStop(): Unit = {
      maybeVault.map(_.dispose())
    }
  }
}

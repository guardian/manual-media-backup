package streamcomponents

import java.time.{Instant, ZoneId, ZonedDateTime}

import akka.stream.{Attributes, FlowShape, Inlet, Outlet, UniformFanOutShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.BackupEntry
import org.slf4j.LoggerFactory

/**
  * checks if the mtime of the backup is earlier than the mtime of the source.  If so, it needs backup and the [[BackupEntry]] is
  * pushed to "yes"; otherwise the [[BackupEntry]] is pushed to "no".
  */
class NeedsBackupSwitch extends GraphStage[UniformFanOutShape[BackupEntry, BackupEntry]] {
  private val in:Inlet[BackupEntry] = Inlet.create("NeedsBackupSwitch.in")
  private val yes:Outlet[BackupEntry] = Outlet.create("NeedsBackupSwitch.yes")
  private val no:Outlet[BackupEntry] = Outlet.create("NeedsBackupSwitch.no")

  override def shape: UniformFanOutShape[BackupEntry, BackupEntry] = new UniformFanOutShape[BackupEntry, BackupEntry](in, Array(yes,no))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        elem.maybeObjectMatrixEntry.flatMap(_.fileAttribues) match {
          case Some(omAttributes)=>try {
            val f = elem.originalPath.toFile
            val fileLastModified: ZonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(f.lastModified()), ZoneId.systemDefault())
            val omLastModified = omAttributes.mtime

            logger.info(s"File ${elem.originalPath} was modified at $fileLastModified but backup was modified at $omLastModified")

            if (fileLastModified.isAfter(omLastModified)) {
              logger.info(s"File ${elem.originalPath} needs backup")
              push(yes, elem)
            } else {
              logger.info(s"File ${elem.originalPath} does not need backup")
              push(no, elem)
            }
          } catch {
            case err:Throwable=>
              logger.error(s"Could not check age of ${elem.originalPath}: ", err)
              failStage(err)
          }
          case None=>
            logger.error(s"Could not check age of file ${elem.originalPath} at ${elem.maybeObjectMatrixEntry} because I could not get the file attributes")
            failStage(new RuntimeException("Could not check age of file"))
        }
      }
    })

    setHandler(no, new AbstractOutHandler {
      override def onPull(): Unit = if(!hasBeenPulled(in)) pull(in)
    })

    setHandler(yes, new AbstractOutHandler {
      override def onPull(): Unit = if(!hasBeenPulled(in)) pull(in)
    })
  }
}

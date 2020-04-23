package streamcomponents

import java.nio.file.attribute.{BasicFileAttributes, PosixFileAttributes}
import java.nio.file.{Files, LinkOption, Path}
import java.time.{Instant, ZoneId, ZonedDateTime}

import akka.stream.{Attributes, Inlet, Materializer, Outlet, UniformFanOutShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import helpers.UnixStat
import models.BackupEntry
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * checks if the mtime of the backup is earlier than the mtime of the source.  If so, it needs backup and the [[BackupEntry]] is
  * pushed to "yes"; otherwise the [[BackupEntry]] is pushed to "no".
  */
class NeedsBackupSwitch(implicit mat:Materializer) extends GraphStage[UniformFanOutShape[BackupEntry, BackupEntry]] {
  private val in:Inlet[BackupEntry] = Inlet.create("NeedsBackupSwitch.in")
  private val yes:Outlet[BackupEntry] = Outlet.create("NeedsBackupSwitch.yes")
  private val no:Outlet[BackupEntry] = Outlet.create("NeedsBackupSwitch.no")

  override def shape: UniformFanOutShape[BackupEntry, BackupEntry] = new UniformFanOutShape[BackupEntry, BackupEntry](in, Array(yes,no))

  /**
    * use the unix `stat` utility to
    * @param forPath
    * @return
    */
  def getModifiedTime(forPath:Path):Future[Either[ZonedDateTime, String]] = {
    UnixStat.getStatInfo(forPath).map(result=>{
      val maybeMtime = result.get("Modify")
      val maybeCtime = result.get("Change")

      (maybeMtime, maybeCtime) match {
        case (Some(mtime),Some(ctime))=>
          if(mtime.isAfter(ctime)) Right(mtime) else Right(ctime)
        case (None,Some(ctime))=>Right(ctime)
        case (Some(mtime),None)=>Right(mtime)
        case (None,None)=>
          Left(s"Could not determine an mtime or a ctime for $forPath")
      }
    }).recover({
      case err:Throwable=>
        Left(s"getStatInfo crashed with: $err")
    })
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        elem.maybeObjectMatrixEntry.flatMap(_.fileAttribues) match {
          case Some(omAttributes)=>try {
            val f = elem.originalPath.toFile

            val fileLastModified: ZonedDateTime = if(f.lastModified()==0L){
              logger.warn(s"${elem.originalPath} has an invalid modification time, setting it to NOW")
              ZonedDateTime.now()
            } else {
              ZonedDateTime.ofInstant(Instant.ofEpochMilli(f.lastModified()), ZoneId.systemDefault())
            }

            val omLastModified = omAttributes.mtime

            val maybeBeingWritten:Option[Boolean] = elem.maybeObjectMatrixEntry.flatMap(_.attributes).flatMap(_.boolValues.get("GNM_BEING_WRITTEN"))

            logger.info(s"File ${elem.originalPath} was modified at $fileLastModified and backup was modified at $omLastModified")

            if(maybeBeingWritten.contains(true)) {
              logger.info(s"File ${elem.originalPath} has 'being written' flag, assuming a previous run crashed and it must be updated")
              push(yes, elem)
            } else if (fileLastModified.isAfter(omLastModified)) {
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

package streamcomponents

import java.nio.file.{Files, Path}
import java.time.{Instant, ZoneId, ZonedDateTime}

import akka.stream.{Attributes, Inlet, Outlet, UniformFanOutShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import helpers.JNAUnixStat
import models.BackupEntry
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * checks if the mtime of the backup is earlier than the mtime of the source.  If so, it needs backup and the [[BackupEntry]] is
  * pushed to "yes"; otherwise the [[BackupEntry]] is pushed to "no".
  */
class NeedsBackupSwitch extends GraphStage[UniformFanOutShape[BackupEntry, BackupEntry]] {
  private val in:Inlet[BackupEntry] = Inlet.create("NeedsBackupSwitch.in")
  private val yes:Outlet[BackupEntry] = Outlet.create("NeedsBackupSwitch.yes")
  private val no:Outlet[BackupEntry] = Outlet.create("NeedsBackupSwitch.no")
  private val outerLogger = LoggerFactory.getLogger(getClass)
  override def shape: UniformFanOutShape[BackupEntry, BackupEntry] = new UniformFanOutShape[BackupEntry, BackupEntry](in, Array(yes,no))

  /**
    * internal method that uses JNA (Java Native Access) to call out to the Linux platform's libc and get native filesystem
    * stat info. This seems to be the only way to get the actual ctime value to compare it to mtime.
    * We have found a small number of files where the mtime is overridden to an earlier time than the actual data write which causes
    * the file not to be backed up on change. To get around this, if the ctime (metadata change time) is later than the mtime (file media
    * change time) then we use the ctime as the "last modified time" instead
    * @param filePath java.nio.Path representing the file to check
    * @return either the modification time as described above or None if an error occurred. Errors are trapped and logged internally.
    */
  protected def getLastModifiedLinuxNative(filePath:Path):Option[ZonedDateTime] = {
    Try { JNAUnixStat.Stat(filePath.toString) } match {
      case Success(statInfo)=>
        //we don't need nanosecond precision here
        if(statInfo.st_mtime.tv_sec.longValue()>statInfo.st_ctime.tv_sec.longValue()) {
          Some(ZonedDateTime.ofInstant(Instant.ofEpochSecond(statInfo.st_mtime.tv_sec.longValue(), statInfo.st_mtime.tv_nsec.longValue()), ZoneId.systemDefault()))
        } else {
          //it's ok to handle the case where the seconds are equal by choosing either
          Some(ZonedDateTime.ofInstant(Instant.ofEpochSecond(statInfo.st_ctime.tv_sec.longValue(), statInfo.st_ctime.tv_nsec.longValue()), ZoneId.systemDefault()))
        }
      case Failure(err)=>
        outerLogger.error(s"Failed to retrieve native Linux stat info for ${filePath.toString}, falling back to Java implementation. To silence this error and not attempt native stat, set -DnoNativeTime when running the program.", err)
        None
    }
  }

  /**
    * Internal method that uses java.io calls to get lastModifiedTime.
    * This corresponds to the Linux system's mtime.
    * It is fine for over 99% of cases but we have encountered situations where the mtime is incorrectly set too early
    * which causes files that should be backed up to be skipped.  The ctime value shows this so we use whichever is later,
    * ctime or mtime, via getLastModifiedLinuxNative. This Java interface is kept for backwards compatibility.
    * @param path
    * @return
    */
  protected def getLastModifiedJava(path: Path):Option[ZonedDateTime] = {
    val f = path.toFile
    if(f.lastModified()==0L){
      None
    } else {
      Some(ZonedDateTime.ofInstant(Instant.ofEpochMilli(f.lastModified()), ZoneId.systemDefault()))
    }
  }

  def getLastModified(filePath:Path):Option[ZonedDateTime] = {
    if(sys.props.get("noNativeTime").isDefined){
      return getLastModifiedJava(filePath)
    }

    val maybeNativeTime = if(JNAUnixStat.isProbablySupported) {
      getLastModifiedLinuxNative(filePath)
    } else {
      None
    }

    maybeNativeTime match {
      case time@Some(_)=>time
      case None=>
        outerLogger.error(s"Could not get linux nativetime for $filePath. To silence this warning set -DnoNativeTime when running the app.")
        getLastModifiedJava(filePath)
    }
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        elem.maybeObjectMatrixEntry.flatMap(_.fileAttribues) match {
          case Some(omAttributes)=>try {
            val fileLastModified = getLastModified(elem.originalPath) match {
              case Some(time)=>time
              case None=>
                outerLogger.warn(s"${elem.originalPath} has an invalid modification time, setting it to NOW")
                ZonedDateTime.now()
            }

            val f = elem.originalPath.toFile
            val omLastModified = omAttributes.mtime

            val maybeBeingWritten:Option[Boolean] = elem.maybeObjectMatrixEntry.flatMap(_.attributes).flatMap(_.boolValues.get("GNM_BEING_WRITTEN"))

            logger.info(s"File ${elem.originalPath} was modified at $fileLastModified and backup was modified at $omLastModified")

            if(maybeBeingWritten.contains(true)) {
              logger.info(s"File ${elem.originalPath} has 'being written' flag, assuming a previous run crashed and it must be updated")
              push(yes, elem)
            } else if (fileLastModified.isAfter(omLastModified)) {
              if(f.length()==omAttributes.size){  //__mxs_length is always lifted by findByFilename and put into the attributes
                                                  // (called from CheckOMFile) so we have it here.
                                                  // if there is an error then the _whole_ of omAttributes is None and we are in the other case
                logger.info(s"File ${elem.originalPath} has a timestamp suggesting it needs backup but the existing file is the same size, skipping")
                push(no, elem)
              } else {
                logger.info(s"File ${elem.originalPath} needs backup, timestamp and file size mismatch")
                push(yes, elem)
              }
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

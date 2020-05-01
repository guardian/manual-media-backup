package helpers

import java.nio.file.{Files, LinkOption, Path}
import java.time.ZonedDateTime

import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.sys.process.Process
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Helper class to run the unix `stat` utility against a file to reveal information about it
  * why do we need to do this?
  * well, it appears that some files' mtimes are not getting updated correctly and are stuck a long way behind the 'change'
  * time. this means that they are not getting picked up for backup when they should.
  * java does not seem to allow us direct access to unix mtime/atime/ctime but instead applies some indirections via e.g. lastModifiedTime
  * which should usually give mtime. However there seems no way to actually get the ctime via these calls since they must be portable between
  * OS.
  * therefore we fall back to running a unix command and parsing the output; which kinda makes me quesy but I can't fina another way around
  * right now
  */
object UnixStat {
  private val logger = LoggerFactory.getLogger(getClass)
  private val getTimeRe = "^\\s*(\\w+):\\s*(\\d.*)$".r

  /**
    * runs the external unix `stat` command against the given file and returns the ctime, mtime and atime
    * as a Map
    * @param forFile file path to run against
    * @param mat implicitly provided ActorMaterializer
    * @return a Future, containing a Map with the times or a failure
    */
  def getStatInfo(forFile:Path)(implicit mat:Materializer):Future[Map[String,ZonedDateTime]] = {
    val sourceFuture = Future {
      val proc = Process("/usr/bin/stat", Seq(forFile.toString))
      val s = proc.lineStream
      akka.stream.scaladsl.Source.fromIterator(() => s.iterator)
    }

    sourceFuture.flatMap(
      _.map(line=>{
        val matches = getTimeRe.findAllIn(line)
        if(matches.groupCount==2){
          try {
            val time = ZonedDateTime.parse(matches.group(2))
            Some(matches.group(1) -> time)
          } catch {
            case err:Throwable=>
              logger.error(s"Could not convert $line which matches regex into a time: ",err)
              None
          }
        } else {
          logger.debug(s"line $line did not match")
          None
        }
      })
        .filter(_.isDefined)
        .map(_.get)
        .toMat(Sink.fold(Map[String,ZonedDateTime]())((acc,elem)=>acc ++ Map(elem)))(Keep.right)
        .run()
    )
  }

  def anotherGetStatInfo(file:Path) = {
    val results = Files.readAttributes(file, "*", LinkOption.NOFOLLOW_LINKS)
    results
  }
}

package models

import java.time.ZonedDateTime

import akka.actor.Actor
import org.slf4j.LoggerFactory

import scala.collection.mutable

object BackupEstimateGroup {
  trait BEMsg

  case class AddToGroup(entry: BackupEstimateEntry) extends BEMsg
  case class FindEntryFor(fileName:String)
  case object QuerySize extends BEMsg
  case object DumpContent extends BEMsg

  case class SizeReturn(count:Int) extends BEMsg
  case class ContentReturn(content:Map[Int,Map[Int,Map[Int,Seq[BackupEstimateEntry]]]]) extends BEMsg
  case class FoundEntry(entries:Seq[BackupEstimateEntry]) extends BEMsg
  case object NotFoundEntry extends BEMsg
}

/**
  * actor implementation to manage a cache of filename->metadata mappings, so it's nice and threadsafe
  */
class BackupEstimateGroup extends Actor {
  private val logger = LoggerFactory.getLogger(getClass)
  import BackupEstimateGroup._

  type MutMap[A,B] = scala.collection.mutable.Map[A,B]

  protected var fileNameMap:MutMap[String,Seq[BackupEstimateEntry]] = mutable.HashMap()

  override def receive: Receive = {
    //add an entry to our datastore
    case AddToGroup(entry)=>
      try {
        fileNameMap(entry.filePath) = fileNameMap.getOrElse(entry.filePath, Seq()) :+ entry
        sender() ! akka.actor.Status.Success( () )
      } catch {
        case err:Throwable=>
          logger.error("Could not update map group: ", err)
          sender() ! akka.actor.Status.Failure(err)
      }

    //search the data for a given entry
    case FindEntryFor(filePath:String)=>
      val results = fileNameMap.getOrElse(filePath, Seq())

      if(results.nonEmpty){
        sender() ! FoundEntry(results)
      } else {
        sender() ! NotFoundEntry
      }

      //return an immutable map of the content
    case DumpContent=>
      sender() ! akka.actor.Status.Success(fileNameMap.toMap)
    //query how many items are present
    case QuerySize=>
      sender() ! SizeReturn(fileNameMap.count(_=>true))
  }
}

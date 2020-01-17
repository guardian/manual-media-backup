package models

import java.time.ZonedDateTime

import akka.actor.Actor
import org.slf4j.LoggerFactory

import scala.collection.mutable

object BackupEstimateGroup {
  trait BEMsg

  case class AddToGroup(entry: BackupEstimateEntry) extends BEMsg
  case class FindEntryFor(fileName:String, mTime:ZonedDateTime)
  case object QueryContent extends BEMsg

  case class ContentReturn(content:Map[Int,Map[Int,Map[Int,Seq[BackupEstimateEntry]]]]) extends BEMsg
  case class FoundEntry(entries:Seq[BackupEstimateEntry]) extends BEMsg
  case object NotFoundEntry extends BEMsg
}

class BackupEstimateGroup extends Actor {
  private val logger = LoggerFactory.getLogger(getClass)
  import BackupEstimateGroup._

  type MutMap[A,B] = scala.collection.mutable.Map[A,B]

  protected var timeMap:MutMap[Int,MutMap[Int, MutMap[Int, Seq[BackupEstimateEntry]]]] = mutable.Map()

  override def receive: Receive = {
    //add an entry to our datastore
    case AddToGroup(entry)=>
      try {
        val year = entry.mTime.getYear
        val existingYear = timeMap.getOrElse(year, mutable.Map())
        val month = entry.mTime.getMonth.getValue
        val existingMonth = existingYear.getOrElse(month, mutable.Map())
        val day = entry.mTime.getDayOfMonth
        val existingDay = existingMonth.getOrElse(day, Seq())

        val updatedDay = existingDay :+ entry
        existingMonth(day) = updatedDay
        sender() ! akka.actor.Status.Success
      } catch {
        case err:Throwable=>
          logger.error("Could not update map group: ", err)
          sender() ! akka.actor.Status.Failure(err)
      }

    //search the data for a given entry
    case FindEntryFor(filePath:String, mTime:ZonedDateTime)=>
      val year = mTime.getYear
      val month = mTime.getMonth.getValue
      val day = mTime.getDayOfMonth

      val allResultsForDay = timeMap.get(year).flatMap(_.get(month).flatMap(_.get(day)))
      val results = allResultsForDay.map(_.filter(_.filePath==filePath)).getOrElse(Seq())

      if(results.nonEmpty){
        sender() ! FoundEntry(results)
      } else {
        sender() ! NotFoundEntry
      }
  }
}

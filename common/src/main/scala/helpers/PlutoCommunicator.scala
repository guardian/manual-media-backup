package helpers

import java.nio.file.Path
import java.util.UUID
import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.stream.Materializer
import models.pluto.{AssetFolderRecord, CommissionRecord, DeliverableAssetRecord, DeliverableBundleRecord, MasterRecord, ProjectRecord, WorkingGroupRecord, WorkingGroupRecordDecoder}
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object PlutoCommunicator {
  trait AFHMsg

  case object TestConnection extends AFHMsg
  case class Lookup(forPath:Path) extends AFHMsg
  case class StoreInCache(forPath:Path,result:Option[AssetFolderRecord]) extends AFHMsg

  case class LookupProject(forId:String) extends AFHMsg
  case class CacheProject(forId:String, result:Option[ProjectRecord]) extends AFHMsg
  case class LookupCommission(forId:String) extends AFHMsg
  case class CacheCommission(forId:String, result:Option[CommissionRecord]) extends AFHMsg
  case class LookupWorkingGroup(forId:UUID) extends AFHMsg
  case class CacheWorkingGroups(list:Seq[WorkingGroupRecord],maybeReturnId:Option[UUID]) extends AFHMsg
  case class LookupMaster(forFileName:String) extends AFHMsg
  case class CacheMaster(forFileName:String, result:Seq[MasterRecord]) extends AFHMsg
  case class LookupDeliverableAsset(forFileName:String) extends AFHMsg
  case class CacheDeliverableAsset(forFileName:String, result:Option[DeliverableAssetRecord]) extends AFHMsg
  case class LookupDeliverableBundle(forId:Int) extends AFHMsg
  case class CacheDeliverableBundle(forId:Int, result:Option[DeliverableBundleRecord]) extends AFHMsg

  case object ConnectionWorking extends AFHMsg
  case class FoundAssetFolder(result:Option[AssetFolderRecord]) extends AFHMsg
  case class FoundProject(result:Option[ProjectRecord]) extends AFHMsg
  case class FoundCommission(result:Option[CommissionRecord]) extends AFHMsg
  case class FoundWorkingGroup(result:Option[WorkingGroupRecord]) extends AFHMsg
  case class FoundMaster(result:Seq[MasterRecord]) extends AFHMsg
  case class FoundDeliverableAsset(result:Option[DeliverableAssetRecord]) extends AFHMsg
  case class FoundDeliverableBundle(result:Option[DeliverableBundleRecord]) extends AFHMsg

  case object LookupFailed extends AFHMsg
}

class PlutoCommunicator(override val plutoBaseUri:String, override val plutoSharedSecret:String)
                       (override implicit val system:ActorSystem, override val mat:Materializer) extends Actor with PlutoCommunicatorFuncs {
  import PlutoCommunicator._

  override protected val logger = LoggerFactory.getLogger(getClass)

  private var assetFolderCache: Map[Path, Option[AssetFolderRecord]] = Map()
  private var projectsCache: Map[String, Option[ProjectRecord]] = Map()
  private var commissionsCache: Map[String, Option[CommissionRecord]] = Map()
  private var workingGroupCache: Map[UUID, Option[WorkingGroupRecord]] = Map()
  private var masterCache: Map[String, Seq[MasterRecord]] = Map()
  private var deliverableAssetCache: Map[String, Option[DeliverableAssetRecord]] = Map()
  private var deliverableBundleCache: Map[Int, Option[DeliverableBundleRecord]] = Map()

  protected val ownRef: ActorRef = self

  override def receive: Receive = {
    case TestConnection =>
      val originalSender = sender()
      performCommsCheck.onComplete({
        case Success(_) => originalSender ! ConnectionWorking
        case Failure(err) =>
          logger.error(s"Could not establish communication with pluto: ", err)
          originalSender ! LookupFailed
      })
    case StoreInCache(forPath, assetFolder) =>
      assetFolderCache ++= Map(forPath -> assetFolder)
      sender() ! akka.actor.Status.Success
    case Lookup(forPath) =>
      assetFolderCache.get(forPath) match {
        case Some(assetFolder) =>
          sender() ! FoundAssetFolder(assetFolder)
        case None =>
          val originalSender = sender()
          performAssetFolderLookup(forPath).onComplete({
            case Success(assetFolder) =>
              ownRef ! StoreInCache(forPath, assetFolder)
              originalSender ! FoundAssetFolder(assetFolder)
            case Failure(err) =>
              logger.error(s"Could not look up potential asset folder path ${forPath.toString}: ", err)
              originalSender ! LookupFailed
          })
      }

    case CacheProject(forId, result) =>
      projectsCache ++= Map(forId -> result)
      sender() ! akka.actor.Status.Success
    case LookupProject(forId) =>
      projectsCache.get(forId) match {
        case Some(projectRecord) =>
          sender() ! FoundProject(projectRecord)
        case None =>
          val originalSender = sender()
          performProjectLookup(forId).onComplete({
            case Success(record) =>
              ownRef ! CacheProject(forId, record)
              originalSender ! FoundProject(record)
            case Failure(err) =>
              logger.error(s"Could not look up project with ID $forId: ", err)
              originalSender ! LookupFailed
          })
      }

    case CacheCommission(forId, result) =>
      commissionsCache ++= Map(forId -> result)
      sender() ! akka.actor.Status.Success
    case LookupCommission(forId) =>
      commissionsCache.get(forId) match {
        case Some(record) =>
          sender() ! FoundCommission(record)
        case None =>
          val originalSender = sender()
          performCommissionLookup(forId).onComplete({
            case Success(record) =>
              ownRef ! CacheCommission(forId, record)
              originalSender ! FoundCommission(record)
            case Failure(err) =>
              logger.error(s"Could no look up commission with ID $forId: ", err)
              originalSender ! LookupFailed
          })
      }

    case CacheWorkingGroups(list, maybeReturn) =>
      workingGroupCache = list.map(rec => (rec.uuid -> Some(rec))).toMap
      maybeReturn match {
        case Some(idToReturn) =>
          sender() ! FoundWorkingGroup(workingGroupCache.get(idToReturn).flatten)
        case None =>
          sender() ! akka.actor.Status.Success
      }
    case LookupWorkingGroup(forId) =>
      if (workingGroupCache.isEmpty) {
        val originalSender = sender()
        performWorkingGroupLookup().onComplete({
          case Success(maybeRecords) =>
            //cache the record, perform the lookup on the cached values and then return it to orginalSender
            ownRef.tell(CacheWorkingGroups(maybeRecords.getOrElse(Seq()), Some(forId)), originalSender)
          case Failure(err) =>
            logger.error(s"Could not look up working groups: ", err)
            originalSender ! LookupFailed
        })
      } else {
        sender() ! FoundWorkingGroup(workingGroupCache.get(forId).flatten)
      }

    case CacheMaster(fileName, result) =>
      masterCache += (fileName -> result)
      sender() ! akka.actor.Status.Success
    case LookupMaster(fileName) =>
      masterCache.get(fileName) match {
        case Some(cachedRecord) =>
          sender() ! FoundMaster(cachedRecord)
        case None =>
          val originalSender = sender()
          performMasterLookup(fileName).onComplete({
            case Success(maybeRecords) =>
              ownRef ! CacheMaster(fileName, maybeRecords)
              originalSender ! FoundMaster(maybeRecords)
            case Failure(err) =>
              logger.error(s"Could not look up master $fileName: ", err)
              originalSender ! LookupFailed
          })
      }

    case CacheDeliverableAsset(fileName, result) =>
      deliverableAssetCache += (fileName -> result)
      sender() ! akka.actor.Status.Success
    case LookupDeliverableAsset(fileName) =>
      deliverableAssetCache.get(fileName) match {
        case Some(cachedRecord) =>
          sender() ! FoundDeliverableAsset(cachedRecord)
        case None =>
          val originalSender = sender()
          performDeliverableLookup(fileName).onComplete({
            case Success(maybeRecord) =>
              ownRef ! CacheDeliverableAsset(fileName, maybeRecord)
              originalSender ! FoundDeliverableAsset(maybeRecord)
            case Failure(err) =>
              logger.error(s"Could not look up deliverables for $fileName: ", err)
              originalSender ! LookupFailed
          })
      }

    case CacheDeliverableBundle(forId, result) =>
      deliverableBundleCache += (forId -> result)
      sender() ! akka.actor.Status.Success
    case LookupDeliverableBundle(forId) =>
      deliverableBundleCache.get(forId) match {
        case Some(cachedRecord) =>
          sender() ! FoundDeliverableBundle(cachedRecord)
        case None =>
          val originalSender = sender()
          performDelvierableBundleLookup(forId).onComplete({
            case Success(maybeRecord) =>
              ownRef ! CacheDeliverableBundle(forId, maybeRecord)
              originalSender ! FoundDeliverableBundle(maybeRecord)
            case Failure(err) =>
              logger.error(s"Could not look up deliverable bundle with ID $forId: ", err)
              originalSender ! LookupFailed
          })
      }
  }
}


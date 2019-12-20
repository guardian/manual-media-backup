package helpers

import java.net.URLEncoder
import java.nio.file.Path
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.http.javadsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{headers, _}
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.ByteString
import io.circe.parser
import io.circe.syntax._
import io.circe.generic.auto._
import models.pluto.{AssetFolderRecord, CommissionRecord, DeliverableAssetRecord, DeliverableBundleRecord, MasterRecord, ProjectRecord, WorkingGroupRecord}
import org.slf4j.LoggerFactory

import scala.collection.parallel.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
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

class PlutoCommunicator(plutoBaseUri:String, plutoUser:String, plutoPass:String)(implicit system:ActorSystem, mat:Materializer) extends Actor {
  import PlutoCommunicator._
  private val logger = LoggerFactory.getLogger(getClass)

  private var assetFolderCache:Map[Path, Option[AssetFolderRecord]] = Map()
  private var projectsCache:Map[String,Option[ProjectRecord]] = Map()
  private var commissionsCache:Map[String, Option[CommissionRecord]] = Map()
  private var workingGroupCache:Map[UUID, Option[WorkingGroupRecord]] = Map()
  private var masterCache:Map[String,Seq[MasterRecord]] = Map()
  private var deliverableAssetCache:Map[String,Option[DeliverableAssetRecord]] = Map()
  private var deliverableBundleCache:Map[Int,Option[DeliverableBundleRecord]] = Map()

  protected val ownRef:ActorRef = self

  override def receive: Receive = {
    case TestConnection=>
      val originalSender = sender()
      performCommsCheck.onComplete({
        case Success(_)=>originalSender ! ConnectionWorking
        case Failure(err)=>
          logger.error(s"Could not establish communication with pluto: ", err)
          originalSender ! LookupFailed
      })
    case StoreInCache(forPath, assetFolder)=>
      assetFolderCache ++= Map(forPath->assetFolder)
      sender() ! akka.actor.Status.Success
    case Lookup(forPath)=>
      assetFolderCache.get(forPath) match {
        case Some(assetFolder)=>
          sender() ! FoundAssetFolder(assetFolder)
        case None=>
          val originalSender = sender()
          performAssetFolderLookup(forPath).onComplete({
            case Success(assetFolder)=>
              ownRef ! StoreInCache(forPath, assetFolder)
              originalSender ! FoundAssetFolder(assetFolder)
            case Failure(err)=>
              logger.error(s"Could not look up potential asset folder path ${forPath.toString}: ", err)
              originalSender ! LookupFailed
          })
      }

    case CacheProject(forId, result)=>
      projectsCache ++= Map(forId->result)
      sender() ! akka.actor.Status.Success
    case LookupProject(forId)=>
      projectsCache.get(forId) match {
        case Some(projectRecord)=>
          sender() ! FoundProject(projectRecord)
        case None=>
          val originalSender = sender()
          performProjectLookup(forId).onComplete({
            case Success(record)=>
              ownRef ! CacheProject(forId, record)
              originalSender ! FoundProject(record)
            case Failure(err)=>
              logger.error(s"Could not look up project with ID $forId: ", err)
              originalSender ! LookupFailed
          })
      }

    case CacheCommission(forId, result)=>
      commissionsCache ++= Map(forId->result)
      sender() ! akka.actor.Status.Success
    case LookupCommission(forId)=>
      commissionsCache.get(forId) match {
        case Some(record)=>
          sender() ! FoundCommission(record)
        case None=>
          val originalSender = sender()
          performCommissionLookup(forId).onComplete({
            case Success(record)=>
              ownRef ! CacheCommission(forId, record)
              originalSender ! FoundCommission(record)
            case Failure(err)=>
              logger.error(s"Could no look up commission with ID $forId: ", err)
              originalSender ! LookupFailed
          })
      }

    case CacheWorkingGroups(list, maybeReturn)=>
      workingGroupCache = list.map(rec=>(rec.uuid->Some(rec))).toMap
      maybeReturn match {
        case Some(idToReturn)=>
          sender() ! FoundWorkingGroup(workingGroupCache.get(idToReturn).flatten)
        case None=>
          sender() ! akka.actor.Status.Success
      }
    case LookupWorkingGroup(forId)=>
      if(workingGroupCache.isEmpty){
        val originalSender = sender()
        performWorkingGroupLookup().onComplete({
          case Success(maybeRecords)=>
            //cache the record, perform the lookup on the cached values and then return it to orginalSender
            ownRef.tell(CacheWorkingGroups(maybeRecords.getOrElse(Seq()), Some(forId)), originalSender)
          case Failure(err)=>
            logger.error(s"Could not look up working groups: ", err)
            originalSender ! LookupFailed
        })
      } else {
        sender() ! FoundWorkingGroup(workingGroupCache.get(forId).flatten)
      }

    case CacheMaster(fileName, result)=>
      masterCache += (fileName -> result)
      sender() ! akka.actor.Status.Success
    case LookupMaster(fileName)=>
      masterCache.get(fileName) match {
        case Some(cachedRecord)=>
          sender() ! FoundMaster(cachedRecord)
        case None=>
          val originalSender = sender()
          performMasterLookup(fileName).onComplete({
            case Success(maybeRecords)=>
              ownRef ! CacheMaster(fileName, maybeRecords)
              originalSender ! FoundMaster(maybeRecords)
            case Failure(err)=>
              logger.error(s"Could not look up master $fileName: ", err)
              originalSender ! LookupFailed
          })
      }

    case CacheDeliverableAsset(fileName, result)=>
      deliverableAssetCache += (fileName->result)
      sender() ! akka.actor.Status.Success
    case LookupDeliverableAsset(fileName)=>
      deliverableAssetCache.get(fileName) match {
        case Some(cachedRecord)=>
          sender() ! FoundDeliverableAsset(cachedRecord)
        case None=>
          val originalSender = sender()
          performDeliverableLookup(fileName).onComplete({
            case Success(maybeRecord)=>
              ownRef ! CacheDeliverableAsset(fileName, maybeRecord)
              originalSender ! FoundDeliverableAsset(maybeRecord)
            case Failure(err)=>
              logger.error(s"Could not look up deliverables for $fileName: ", err)
              originalSender ! LookupFailed
          })
      }

    case CacheDeliverableBundle(forId, result)=>
      deliverableBundleCache += (forId->result)
      sender() ! akka.actor.Status.Success
    case LookupDeliverableBundle(forId)=>
      deliverableBundleCache.get(forId) match {
        case Some(cachedRecord)=>
          sender() ! FoundDeliverableBundle(cachedRecord)
        case None=>
          val originalSender = sender()
          performDelvierableBundleLookup(forId).onComplete({
            case Success(maybeRecord)=>
              ownRef ! CacheDeliverableBundle(forId, maybeRecord)
              originalSender ! FoundDeliverableBundle(maybeRecord)
            case Failure(err)=>
              logger.error(s"Could not look up deliverable bundle with ID $forId: ", err)
              originalSender ! LookupFailed
          })
      }
  }

  /**
    * internal method that consumes a given response entity to a String
    * @param entity ResponseEntity object
    * @return a Future containing the String of the content
    */
  def consumeResponseEntity(entity:ResponseEntity) = {
    val sink = Sink.reduce[ByteString]((acc,elem)=>acc ++ elem)
    entity.dataBytes.toMat(sink)(Keep.right).run().map(_.utf8String)
  }

  /**
    * convenience method that consumes a given response entity and parses it into a Json object
    * @param entity ResponseEntity object
    * @return a Future containing either the ParsingFailure error or the parsed Json object
    */
  def consumeResponseEntityJson(entity:ResponseEntity) = consumeResponseEntity(entity)
    .map(io.circe.parser.parse)

  /* extract call to static object to make testing easier */
  def callHttp = Http()

  /**
    * internal method that performs a call to pluto, handles response codes/retries and unmarshals reutrned JSON to a domain object.
    * If the server returns a 200 response then the content is parsed as JSON and unmarshalled into the given object
    * If the server returns a 404 response then None is returned
    * If the server returns a 403 or a 400 then a failed future is returned
    * If the server returns a 500, 502, 503 or 504 then the request is retried after (attempt*0.5) seconds up till 10 attempts
    * @param req constructed akka HttpRequest to perform
    * @param attempt attempt counter, you don't need to specify this when calling
    * @tparam T the type of domain object to unmarshal the response into. There must be an io.circe.Decoder in-scope for this kind of object.
    *           if the unmarshalling fails then a failed Future is returned
    * @return a Future containing an Option with either the unmarshalled domain object or None
    */
  protected def callToPluto[T:io.circe.Decoder](req:HttpRequest, attempt:Int=1):Future[Option[T]] = if(attempt>10) {
    Future.failed(new RuntimeException("Too many retries, see logs for details"))
  } else {
    logger.debug(s"Request URL is ${req.uri.toString()}")
    val auth:HttpHeader = Authorization(headers.BasicHttpCredentials(plutoUser, plutoPass))
    val updatedReq = req.copy(headers = scala.collection.immutable.Seq(auth)) //add in the authorization header

    callHttp.singleRequest(updatedReq).flatMap(response => {
      val contentBody = consumeResponseEntity(response.entity)

      response.status.intValue() match {
        case 200 =>
          contentBody
            .map(io.circe.parser.parse)
            .map(_.flatMap(_.as[T]))
            .map({
              case Left(err) =>
                logger.error(s"Problematic response: ${contentBody.value}")
                throw new RuntimeException("Could not understand server response: ", err)
              case Right(data) => Some(data)
            })
        case 404 =>
          Future(None)
        case 403 =>
          throw new RuntimeException(s"Pluto said permission denied.")  //throwing an exception here will fail the future,
                                                                        //which is picked up in onComplete in the call
        case 400 =>
          contentBody.map(body=>throw new RuntimeException(s"Pluto returned bad data error: $body"))
        case 500|502|503|504 =>
          contentBody.flatMap(body=> {
            logger.error(s"Pluto returned a server error: $body. Retrying...")
            Thread.sleep(500 * attempt)
            callToPluto(req, attempt + 1)
          })
      }
    })
  }

  def performCommsCheck:Future[Unit] = {
    val req = HttpRequest(uri=s"$plutoBaseUri/gnm_asset_folder/lookup?path=invalid")
    callToPluto[AssetFolderRecord](req).map(_=>())  //we are not interested in the result, it should be None, but if we didn't get an error that's good enough
  }
  /**
    * asks Pluto for information on the given (potential) asset folder path.
    *
    * @param forPath java.nio.file.Path representing the path to look up
    * @return a Future containing an AssetFolderResponse, or if there was an error a failed Future (catch this with
    *         .recover() or .onComplete)
    */
  def performAssetFolderLookup(forPath:Path):Future[Option[AssetFolderRecord]] = {
    val req = HttpRequest(uri=s"$plutoBaseUri/gnm_asset_folder/lookup?path=${URLEncoder.encode(forPath.toString, "UTF-8")}")
    callToPluto[AssetFolderRecord](req)
  }

  def performCommissionLookup(forId:String) = {
    import LocalDateTimeEncoder._
    val req = HttpRequest(uri=s"$plutoBaseUri/commission/api/$forId")
    callToPluto[CommissionRecord](req)
  }

  def performProjectLookup(forId:String) = {
    import LocalDateTimeEncoder._
    val req = HttpRequest(uri=s"$plutoBaseUri/project/api/$forId")
    callToPluto[ProjectRecord](req)
  }

  def performWorkingGroupLookup() = {
    val req = HttpRequest(uri=s"$plutoBaseUri/commission/api/groups/")
    callToPluto[Seq[WorkingGroupRecord]](req)
  }

  def performMasterLookup(forFileName:String) = {
    import LocalDateTimeEncoder._
    val req = HttpRequest(uri=s"$plutoBaseUri/master/api/byFileName/?filename=${URLEncoder.encode(forFileName, "UTF-8")}")
    callToPluto[Seq[MasterRecord]](req).map(_.getOrElse(Seq()))
  }

  def performDeliverableLookup(forFileName:String) = {
    import LocalDateTimeEncoder._
    val req = HttpRequest(uri=s"$plutoBaseUri/deliverables/api/byFileName/?filename=${URLEncoder.encode(forFileName, "UTF-8")}")
    callToPluto[DeliverableAssetRecord](req)
  }

  def performDelvierableBundleLookup(forId:Int) = {
    import LocalDateTimeEncoder._
    val req = HttpRequest(uri=s"$plutoBaseUri/deliverables/api/$forId")
    callToPluto[DeliverableBundleRecord](req)
  }
}

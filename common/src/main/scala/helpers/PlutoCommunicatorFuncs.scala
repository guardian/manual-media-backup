package helpers

import java.net.URLEncoder
import java.nio.file.Path
import java.security.MessageDigest
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{headers, _}
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, GenericHttpCredentials, RawHeader}
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.ByteString
import auth.HMAC
import io.circe.syntax._
import models.pluto.{AssetFolderRecord, CommissionRecord, DeliverableAssetRecord, DeliverableBundleRecord, MasterRecord, ProjectRecord, WorkingGroupRecord}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * extract called functions out of the actor class itself so we can call them directly during testing
  */
trait PlutoCommunicatorFuncs {
  protected val logger:org.slf4j.Logger
  implicit val system:ActorSystem
  implicit val mat:Materializer

  val plutoBaseUri:String
  val plutoSharedSecret:String

  private val multiSlashRemover = "^/{2,}".r

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

  def contentBodyToJson[T:io.circe.Decoder](contentBody:Future[String]) = contentBody
    .map(io.circe.parser.parse)
    .map(_.map(json=>(json \\ "result").headOption.getOrElse(json)))  //if the actual object is hidden under a "result" field take that
    .map(_.flatMap(_.as[T]))
    .map({
      case Left(err) =>
        logger.error(s"Problematic response: ${contentBody.value}")
        throw new RuntimeException("Could not understand server response: ", err)
      case Right(data) => Some(data)
    })

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
    val checksumBytes = MessageDigest.getInstance("SHA-384").digest("".getBytes)
    val checksumString = checksumBytes.map("%02x".format(_)).mkString
    val token = HMAC.calculateHmac(
      "",
      checksumString,
      "GET",
      multiSlashRemover.replaceAllIn(req.uri.path.toString(), "/"),
      plutoSharedSecret,
    )

    if(token.isEmpty) Future.failed(new RuntimeException("could not build authorization"))

    val auth:HttpHeader = RawHeader("Authorization", s"HMAC ${token.get}")
    val checksum = RawHeader("Digest",s"SHA-384=$checksumString")

    val updatedReq = req.copy(headers = scala.collection.immutable.Seq(auth, checksum)) //add in the authorization header

    callHttp.singleRequest(updatedReq).flatMap(response => {
      val contentBody = consumeResponseEntity(response.entity)

      response.status.intValue() match {
        case 200 =>
          contentBodyToJson(contentBody)
        case 404 =>
          Future(None)
        case 403 =>
          throw new RuntimeException(s"Pluto said permission denied.")  //throwing an exception here will fail the future,
        //which is picked up in onComplete in the call
        case 400 =>
          contentBody.map(body=>throw new RuntimeException(s"Pluto returned bad data error: $body"))
        case 301 =>
          logger.warn(s"Received unexpected redirect from pluto to ${response.getHeader("Location")}")
          val h = response.getHeader("Location")
          if(h.isPresent){
            val newUri = h.get()
            logger.info(s"Redirecting to ${newUri.value()}")
            val updatedReq = req.copy(uri=Uri(newUri.value()))
            callToPluto(updatedReq, attempt+1)
          } else {
            throw new RuntimeException("Unexpected redirect without location")
          }
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
    import io.circe.generic.auto._
    val req = HttpRequest(uri=s"$plutoBaseUri/pluto-core/api/assetfolder/lookup?path=invalid")
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
    import io.circe.generic.auto._
    val req = HttpRequest(uri=s"$plutoBaseUri/pluto-core/api/assetfolder/lookup?path=${URLEncoder.encode(forPath.toString, "UTF-8")}")
    callToPluto[AssetFolderRecord](req)
  }

  def performCommissionLookup(forId:Int) = {
    import io.circe.generic.auto._
    import LocalDateTimeEncoder._
    val req = HttpRequest(uri=s"$plutoBaseUri/pluto-core/api/pluto/commission/$forId")
    callToPluto[CommissionRecord](req)
  }

  def performProjectLookup(forId:Int) = {
    import io.circe.generic.auto._
    import LocalDateTimeEncoder._
    val req = HttpRequest(uri=s"$plutoBaseUri/pluto-core/api/project/$forId")
    val result = callToPluto[ProjectRecord](req)
    result.onComplete({
      case Success(maybeRecord)=>logger.info(s"performProjectLookup got $maybeRecord for $forId")
      case Failure(err)=>logger.error(s"performProjectLookup failed for $forId: ", err)
    })
    result
  }

  def performWorkingGroupLookup() = {
    import io.circe.generic.auto._
    import LocalDateTimeEncoder._
    val req = HttpRequest(uri=s"$plutoBaseUri/pluto-core/api/workinggroup")
    callToPluto[Seq[WorkingGroupRecord]](req)
  }

  def performDeliverableLookup(forFileName:String) = {
    import io.circe.generic.auto._
    import LocalDateTimeEncoder._
    val req = HttpRequest(uri=s"$plutoBaseUri/deliverables/api/asset/byFileName/?filename=${URLEncoder.encode(forFileName, "UTF-8")}")
    callToPluto[DeliverableAssetRecord](req)
  }

  def performDeliverableBundleLookup(forId:Int) = {
    import io.circe.generic.auto._
    import LocalDateTimeEncoder._
    val req = HttpRequest(uri=s"$plutoBaseUri/deliverables/api/bundle/$forId")
    callToPluto[DeliverableBundleRecord](req)
  }
}

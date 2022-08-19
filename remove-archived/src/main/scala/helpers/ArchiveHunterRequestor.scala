package helpers

import java.net.URLEncoder
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpHeader, HttpRequest}
import akka.stream.Materializer

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import org.slf4j.LoggerFactory
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LoggingMagnet}
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.ByteString
import auth.HMAC.{generateHMAC, logger}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ArchiveHunterRequestor(baseUri:String, key:String)(implicit val system:ActorSystem, implicit val mat:Materializer) {
  private val logger = LoggerFactory.getLogger(getClass)

  private val secretKeySpec = new SecretKeySpec(key.getBytes, "HmacSHA384")

  def currentTimeString = ZonedDateTime.now().format(DateTimeFormatter.ofPattern("E, dd MMM y HH:mm:ss z"))

  def makeAuth(req:HttpRequest) = {
    val httpDate = currentTimeString
    logger.debug(s"date string is $httpDate")
    val stringToSign = s"$httpDate\n0\nstring\nGET\n${req.uri.toString()}"
    logger.debug(s"stringToSign is $stringToSign")

    val mac = Mac.getInstance("HmacSHA384")
    mac.init(secretKeySpec)
    val resultBytes = mac.doFinal(stringToSign.getBytes)

    val signature = Base64.encodeBase64String(resultBytes)
    logger.debug(s"signature is $signature")
    val authHeader = headers.RawHeader("Authorization", s"HMAC $signature")
    logger.debug(s"authHeader is ${authHeader.toString()}")
    val dateHeader = headers.RawHeader("Date",httpDate)
    logger.debug(s"dateHeader is ${dateHeader.toString()}")
    val sumHeader = headers.RawHeader("X-Sha384-Checksum", s"string")
    req.withHeaders(authHeader, dateHeader, sumHeader)
  }

  def decodeParsedData(parsedData:io.circe.Json) = {
    val node = parsedData.hcursor.downField("entries").downArray.first

    val maybeCollectionName = node.downField("bucket").as[String]
    val maybeArchiveHunterId = node.downField("id").as[String]
    val maybeDeleted = node.downField("beenDeleted").as[Boolean]
    val maybeSize = node.downField("size").as[Long]
    if(maybeCollectionName.isLeft || maybeArchiveHunterId.isLeft || maybeDeleted.isLeft){
      val errorDetail = Seq(maybeCollectionName, maybeArchiveHunterId, maybeDeleted).collect({case Left(err)=>err.toString()}).mkString(",")
      Left(s"Could not decode json from ArchiveHunder: $errorDetail")
    } else {
      Right(ArchiveHunterFound(maybeArchiveHunterId.right.get, maybeCollectionName.right.get, maybeSize.right.toOption, maybeDeleted.right.get))
    }
  }

  def lookupRequest(forPath:String):Future[Either[String,ArchiveHunterLookupResult]] = {
    val url = s"$baseUri/api/searchpath?filePath=${URLEncoder.encode(forPath)}"

    logger.debug(s"Going to request $url")

    val req = HttpRequest(uri = url)
    val signedRequest = makeAuth(req)
    def printRequest(req:HttpRequest): Unit = logger.debug(s"${req.method.name} ${req.uri} ${req.headers}")
    DebuggingDirectives.logRequest(LoggingMagnet(_=>printRequest))

    Http().singleRequest(signedRequest).flatMap(response=>{
      response.status match {
        case StatusCodes.NotFound=>
          response.entity.discardBytes()
          Future(Right(ArchiveHunterNotFound))
        case StatusCodes.OK=>
          response.entity.getDataBytes().runWith(Sink.fold(ByteString())((acc,entry)=>acc++entry), mat).map(bytes=>{
            val stringData = bytes.decodeString("UTF-8")
            logger.debug(s"got response $stringData")
            io.circe.parser.parse(stringData) match {
              case Right(parsedData)=>decodeParsedData(parsedData)
              case Left(parseError)=>Left(parseError.toString)
            }
          })
        case _=>
          response.entity.getDataBytes().runWith(Sink.fold(ByteString())((acc,entry)=>acc++entry), mat).map(bytes=>{
            Left(s"Unexpected response ${response.status} from server: ${bytes.decodeString("UTF-8")}")
          })
      }
    })
  }
}

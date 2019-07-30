package vidispine

import java.util.Base64

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import com.softwaremill.sttp._
import com.softwaremill.sttp.akkahttp.AkkaHttpBackend
import models.HttpError
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

object VSCommunicator {
  object OperationType extends Enumeration {
    val GET,PUT,POST,DELETE = Value
  }
}

/**
  * this class provides base functionality to communicate with Vidispine via Akka HTTP. As such it is true non-blocking.
  * @param vsUri base URI to access Vidispine (don't include the /API part). Request URIs are concatenated to this.
  * @param plutoUser username for accessing Vidispine
  * @param plutoPass password for accessing Vidispine
  * @param actorSystem implicitly provided reference to an ActorSystem, for Akka
  */
class VSCommunicator(vsUri:Uri, plutoUser:String, plutoPass:String)(implicit val actorSystem:ActorSystem) {
  private val logger = LoggerFactory.getLogger(getClass)
  import VSCommunicator._

  private def authString:String = Base64.getEncoder.encodeToString(s"$plutoUser:$plutoPass".getBytes)

  protected implicit val sttpBackend:SttpBackend[Future,Source[ByteString, Any]] = AkkaHttpBackend.usingActorSystem(actorSystem)

  /**
    * internal method to perform a request
    * @param operation which request to perform - VSCommunicator.OperationType enumerates them
    * @param uriPath URI to perform the operation on
    * @param maybeXmlString an optional XML body. If this is 'None' then no body is sent on the request
    * @param headers String->string map of headers
    * @param queryParams String->string map of query params
    * @return a Future, with a response containing a stream source to read the body.
    */
  private def sendGeneric(operation:OperationType.Value, uriPath:String, maybeXmlString:Option[String], headers:Map[String,String], queryParams:Map[String,String]):Future[Response[Source[ByteString, Any]]] = {
    val bs = maybeXmlString.map(xmlString=>ByteString(xmlString,"UTF-8"))

    val uriWithPath = vsUri.path(uriPath)
    val uri = queryParams.foldLeft[Uri](uriWithPath)((acc, tuple)=>acc.queryFragment(Uri.QueryFragment.KeyValue(tuple._1,tuple._2)))
    val source:Option[Source[ByteString, Any]] = bs.map(Source.single)

    val hdr = Map(
      "Accept"->"application/xml",
      "Authorization"->s"Basic $authString",
      "Content-Type"->"application/xml"
    ) ++ headers

    logger.debug(s"Got headers, initiating $operation to $uri")

    val op = operation match {
      case OperationType.PUT=>sttp.put(uri)
      case OperationType.GET=>sttp.get(uri)
      case OperationType.DELETE=>sttp.delete(uri)
      case OperationType.POST=>sttp.post(uri)
    }

    val opWithBody = source match {
      case None=>op
      case Some(byteSource)=>op.streamBody(byteSource)
    }

    opWithBody
      .headers(hdr)
      .response(asStream[Source[ByteString, Any]])
      .send()
  }


  private def sendDelete(uriPath:String, headers:Map[String,String], queryParams:Map[String,String]):Future[Response[Source[ByteString, Any]]] = {
    val hdr = Map(
      "Authorization"->s"Basic $authString",
      "Content-Type"->"application/xml"
    ) ++ headers

    val uriWithPath = vsUri.path(uriPath)
    val uri = queryParams.foldLeft[Uri](uriWithPath)((acc, tuple)=>acc.queryFragment(Uri.QueryFragment.KeyValue(tuple._1,tuple._2)))

    logger.debug(s"Got headers, initiating DELETE to $uri")
    sttp
      .delete(uri)
      .headers(hdr)
      .response(asStream[Source[ByteString, Any]])
      .send()
  }

  /**
    * internal method to buffer returned data into a String for parsing
    * @param source Akka source that yields ByteString entries
    * @param materializer implicitly provided stream materializer
    * @param ec implicitly provided execution context for async operations
    * @return a Future, which contains the String of the returned content.
    */
  private def consumeSource(source:Source[ByteString,Any])(implicit materializer: akka.stream.Materializer, ec: ExecutionContext):Future[String] = {
    logger.debug("Consuming returned body")
    val sink = Sink.reduce((acc:ByteString, unit:ByteString)=>acc.concat(unit))
    val runnable = source.toMat(sink)(Keep.right)
    runnable.run().map(_.utf8String)
  }

  /**
    * request any operation to Vidispine
    * @param operationType the operation to perform. VSCOmmunicator.OperationType enumerates the possible values
    * @param uriPath URI to put to. This should include /API, and is concatenated to the `baseUri` contructor parameter
    * @param maybeXmlString optional request body
    * @param headers Map of headers to send to the server. Authorization is automatcially added.
    * @param materializer implicitly provided stream materializer
    * @param ec implicitly provided execution context for async operations
    * @return a Future, containing either an [[HttpError]] instance or a String of the server's response
    */
  def request(operationType: OperationType.Value, uriPath:String,maybeXmlString:Option[String],headers:Map[String,String], queryParams:Map[String,String]=Map(), attempt:Int=0)
             (implicit materializer: akka.stream.Materializer,ec: ExecutionContext):
  Future[Either[HttpError,String]] = sendGeneric(operationType, uriPath, maybeXmlString, headers, queryParams).flatMap({ response=>
    response.body match {
      case Right(source)=>
        logger.debug("Send succeeded")
        consumeSource(source).map(data=>Right(data))
      case Left(errorString)=>
        if(response.code==503){
          val delayTime = if(attempt>6) 60 else 2^attempt
          logger.warn(s"Received 503 from Vidispine. Retrying in $delayTime seconds.")
          Thread.sleep(delayTime*1000)  //FIXME: should do this in a non-blocking way, if possible.
          request(operationType, uriPath, maybeXmlString, headers, queryParams, attempt+1)
        } else {
          VSError.fromXml(errorString) match {
            case Left(unparseableError) =>
              val errMsg = s"Send failed: ${response.code} - $errorString"
              logger.warn(errMsg)
              Future(Left(HttpError(errMsg, response.code)))
            case Right(vsError) =>
              logger.warn(vsError.toString)
              Future(Left(HttpError(vsError.toString, response.code)))
          }
        }
    }})

  /**
    * request a GET operation to Vidispine
    * @param uriPath URI to GET from. This should include /API, and is concatenated to the `baseUri` contructor parameter
    * @param headers Map of headers to send to the server. Authorization is automatcially added.
    * @param queryParams Dictionary of query parameters to add to the string. All keys/values must be strings.
    * @param materializer implicitly provided stream materializer
    * @param ec implicitly provided execution context for async operations
    * @return a Future, containing either an [[HttpError]] instance or a String of the server's response
    */
  def requestGet(uriPath:String, headers:Map[String,String],queryParams:Map[String,String]=Map(),attempt:Int=0)
                (implicit materializer: akka.stream.Materializer,ec: ExecutionContext):
  Future[Either[HttpError,String]] = sendGeneric(OperationType.GET, uriPath, None, headers, queryParams).flatMap({ response=>
    response.body match {
      case Right(source)=>
        logger.debug("Send succeeded")
        consumeSource(source).map(data=>Right(data))
      case Left(errorString)=>
        if(response.code==503){
          val delayTime = if(attempt>6) 60 else 2^attempt
          logger.warn(s"Received 503 from Vidispine on attempt $attempt. Retrying in $delayTime seconds.")
          Thread.sleep(delayTime*1000)  //FIXME: should do this in a non-blocking way, if possible.
          requestGet(uriPath, headers, queryParams, attempt+1)
        } else {
          VSError.fromXml(errorString) match {
            case Left(unparseableError) =>
              val errMsg = s"Send failed: ${response.code} - $errorString"
              logger.warn(errMsg)
              Future(Left(HttpError(errMsg, response.code)))
            case Right(vsError) =>
              logger.warn(vsError.toString)
              Future(Left(HttpError(vsError.toString, response.code)))
          }
        }
    }})

  def requestDelete(uriPath:String, headers:Map[String,String],queryParams:Map[String,String]=Map(),attempt:Int=0)
                   (implicit materializer: akka.stream.Materializer,ec: ExecutionContext):
  Future[Either[HttpError,String]] = sendDelete(uriPath, headers, queryParams).flatMap({ response=>
    response.body match {
      case Right(source)=>
        logger.debug("Send succeeded")
        consumeSource(source).map(data=>Right(data))
      case Left(errorString)=>
        if(response.code==503){
          val delayTime = if(attempt>6) 60 else 2^attempt
          logger.warn(s"Received 503 from Vidispine on attempt $attempt. Retrying in $delayTime seconds.")
          Thread.sleep(delayTime*1000)
          requestGet(uriPath, headers, queryParams, attempt+1)
        } else {
          VSError.fromXml(errorString) match {
            case Left(unparseableError) =>
              val errMsg = s"Send failed: ${response.code} - $errorString"
              logger.warn(errMsg)
              Future(Left(HttpError(errMsg, response.code)))
            case Right(vsError) =>
              logger.warn(vsError.toString)
              Future(Left(HttpError(vsError.toString, response.code)))
          }
        }
    }})
}

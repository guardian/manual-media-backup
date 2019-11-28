package helpers

import java.net.URLEncoder
import java.nio.file.Path

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.ByteString
import io.circe.parser
import io.circe.syntax._
import io.circe.generic.auto._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class AssetFolderResponse ()

class AssetFolder (plutoBaseUri:String, plutoUser:String, plutoPass:String)(implicit system:ActorSystem, mat:Materializer) {
  private val logger = LoggerFactory.getLogger(getClass)

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

  /**
    * asks Pluto for information on the given (potential) asset folder path.
    *
    * @param forPath java.nio.file.Path representing the path to look up
    * @param attempt attempt counter, you don't need to specify this when calling as it defaults to 1
    * @return a Future containing an AssetFolderResponse, or if there was an error a failed Future (catch this with
    *         * .recover() )
    */
  def Lookup(forPath:Path, attempt:Int=1):Future[AssetFolderResponse] = {
    val req = HttpRequest(uri=s"$plutoBaseUri/gnm_asset_folder/lookup?path=${URLEncoder.encode(forPath.toString)}")

    if(attempt>10){
      logger.error(s"Too many attempts, giving up")
      throw new RuntimeException("Too many attempts, giving up")
    } else {
      Http().singleRequest(req).flatMap(response => {
        val contentBody = consumeResponseEntity(response.entity)

        response.status.intValue() match {
          case 200 =>
            contentBody
              .map(io.circe.parser.parse)
              .map(_.flatMap(_.as[AssetFolderResponse]))
              .map({
                case Left(err) => throw new RuntimeException("Could not understand server response: ", err)
                case Right(data) => data
              })
          case 404 =>
            throw new RuntimeException(s"No asset folder was found for $forPath")
          case 403 =>
            throw new RuntimeException(s"Pluto said permission denied.")
          case 400 =>
            throw new RuntimeException(s"Pluto returned bad data error: $contentBody")
          case 500 =>
            logger.error(s"Pluto returned a server error: $contentBody. Retrying...")
            Thread.sleep(500 * attempt)
            Lookup(forPath)
          case 503 =>
            logger.error(s"Pluto returned server not available. Retrying...")
            Thread.sleep(500 * attempt)
            Lookup(forPath, attempt + 1)
          case 504 =>
            logger.error(s"Pluto returned server not available. Retrying...")
            Thread.sleep(500 * attempt)
            Lookup(forPath, attempt + 1)
        }
      })
    }
  }
}

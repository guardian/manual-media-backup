package helpers

import java.io.{File, FileReader}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.stream.Materializer
import akka.stream.scaladsl.{FileIO, Framing, Keep, Sink, Source}
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}


object ListReader {
  /**
    * reads a newline-delimited file list from an HTTP URL. The final list is buffered in memory and returned as a Seq[String].
    * this is called by ListReader.read() to handle any path starting with 'http:'
    * @param uri URI to access
    * @param acceptContentType optional string, if set tell HTTP server that this is the content type we want
    * @param system implicitly provided actor system
    * @param mat implicitly provided materializer
    * @param ec implicitly provided execution context
    * @return a Future containing either a Right with a sequence of strings if the read was successful or a Left with the
    *         content body as a UTF-8 string if the server did not return a 200 Success result. 201 No Content is treated as an error, because we need
    *         some content to work with
    */
  def fromHttp(uri:Uri,acceptContentType:Option[String])(implicit system:ActorSystem, mat:Materializer, ec:ExecutionContext) = {
    val headers:scala.collection.immutable.Seq[HttpHeader] = scala.collection.immutable.Seq(
      acceptContentType.map(ct=>new Accept(scala.collection.immutable.Seq(MediaRange(MediaType.custom(ct,false)))))
    ).collect({case Some(hdr)=>hdr})

    Http().singleRequest(HttpRequest(HttpMethods.GET,uri,headers)).flatMap(response=>{
      response.entity.dataBytes
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
        .map(_.decodeString("UTF-8"))
        .toMat(Sink.fold[Seq[String],String](Seq())((acc,entry)=>acc++Seq(entry)))(Keep.right)
        .run()
        .map(result=>{
          if(response.status!=StatusCodes.OK){
            Right(result)
          } else {
            Left(result.mkString("\n"))
          }
      })
    })
  }

  /**
    * reads a newline-delimited file list from a local file.he final list is buffered in memory and returned as a Seq[String].
    * this is called by ListReader.read() if there is no obvious protocol handler.
    *
    * @param file
    * @param mat
    * @return
    */
  def fromFile(file:File)(implicit mat:Materializer,ec:ExecutionContext) = {
    try {
      FileIO.fromPath(file.toPath)
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
        .map(_.decodeString("UTF-8"))
        .toMat(Sink.fold[Seq[String], String](Seq())((acc, entry) => acc ++ Seq(entry)))(Keep.right)
        .run()
        .map(result=>Right(result))
        .recover({
          case err:Throwable=>
            Left(err.toString)
        })
    } catch {
      case err:Throwable=>
        Future(Left(err.toString))
    }
  }

  /**
    * reads a file list from some path. Anythin starting with http: or https: is downloaded via akka http, otherwise
    * it's treated as a local file
    * @param path path to download
    * @param acceptContentType Optional string to stipulate content type when downloading from a server
    * @param system
    * @param mat
    * @param ec
    * @return a Future, with either an error message in a Left or a sequence of filenames in a Right
    */
  def read(path:String, acceptContentType:Option[String]=None)(implicit system:ActorSystem, mat:Materializer, ec:ExecutionContext) = {
    if(path.startsWith("http:") || path.startsWith("https:")){
      fromHttp(Uri(path), acceptContentType)
    } else {
      fromFile(new File(path))
    }
  }
}

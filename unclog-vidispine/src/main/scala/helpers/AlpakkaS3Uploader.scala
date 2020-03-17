package helpers

import akka.actor.ActorSystem
import akka.stream.{ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph}
import models.{ObjectMatrixEntry, S3Target}
import akka.stream.alpakka.s3._
import akka.http.scaladsl.model.ContentType
import com.om.mxs.client.japi.UserInfo
import org.slf4j.LoggerFactory
import streamcomponents.MatrixStoreFileSource

class AlpakkaS3Uploader(userInfo:UserInfo) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * perform an asynchronous upload from ObjectMatrix to S3
   * @param sourceOid OID of the source file to upload, this must be on the vault as given by the constructing UserInfo
   * @param sourceContentType content type that you want to upload
   * @param to [[S3Target]] object indicating where to upload to
   * @param sys implicitly provided ActorSystem
   * @param mat implicitly provided Materializer
   * @return a Future containing an alpakka MultipartUploadResult describing the operation
   */
  def performS3Upload(sourceOid:String, sourceContentType:String, to:S3Target)(implicit sys:ActorSystem, mat:Materializer) = {
    val realContentType = ContentType.parse(sourceContentType) match {
      case Left(errs)=>
        logger.warn(s"could not parse content type '$sourceContentType': $errs'")
        ContentType.parse("application/octet-stream").right.get
      case Right(ct)=>ct
    }

    val sinkFact = scaladsl.S3.multipartUpload(
      to.bucket,
      to.path,
      realContentType,
    )

    val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new MatrixStoreFileSource(userInfo, sourceOid))
      src ~> sink
      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }
}
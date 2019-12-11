import java.io.{ByteArrayInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{Framing, GraphDSL, RunnableGraph, Sink, Source}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{CannedAccessControlList, ObjectMetadata, PutObjectRequest}
import com.gu.vidispineakka.streamcomponents.{VSItemGetFullMeta, VSItemSearchSource}
import com.gu.vidispineakka.vidispine.{VSCommunicator, VSLazyItem}
import com.softwaremill.sttp.Uri
import org.slf4j.LoggerFactory
import streamComponents.{UploadItemShape, UploadItemThumbnail}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import com.softwaremill.sttp._
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._

object PushProxiesMain {
  private val logger = LoggerFactory.getLogger(getClass)

  lazy val vsPageSize = sys.env.get("VS_PAGE_SIZE").map(_.toInt).getOrElse(100)

  lazy val s3Client = AmazonS3ClientBuilder.standard().build()

  val metaBucket = sys.env.get("META_BUCKET") match {
    case Some(b)=>b
    case None=>throw new RuntimeException("You must specify META_BUCKET in the environment")
  }

  val proxyBucket = sys.env.get("PROXY_BUCKET") match {
    case Some(b)=>b
    case None=>throw new RuntimeException("You must specify PROXY_BUCKET in the environment")
  }

  val mediaBucket = sys.env.get("MEDIA_BUCKET") match {
    case Some(b)=>b
    case None=>throw new RuntimeException("You must specify MEDIA_BUCKET in the environment")
  }

  val vsUrl = sys.env("VIDISPINE_URL")
  val vsUser = sys.env("VIDISPINE_USER")
  val vsPasswd = sys.env("VIDISPINE_PASSWORD")
  val vsSite = sys.env.getOrElse("VIDISPINE_SITE_ID", "VX")

  val proxyShapeNames = Seq("lowres","lowaudio","lowimage")

  /**
    * provide an empty search => match everything
    * @return
    */
  def makeVSSearch = <ItemSearchDocument xmlns="http://xml.vidispine.com/schema/vidispine"></ItemSearchDocument>

  /**
    * callback to upload metadata content directly
    * @param forItem
    * @param metaDoc
    * @return
    */
  def writeMetaCallback(forItem:VSLazyItem, metaDoc:String):Future[Unit] = {
    val destFileName = forItem.getSingle("gnm_asset_filename") match {
      case Some(filePath)=>filePath + ".xml"
      case None=>forItem.itemId + ".xml"
    }
    logger.info(s"Writing metadata to $destFileName on $metaBucket")

    val strm = new ByteArrayInputStream(metaDoc.getBytes(StandardCharsets.UTF_8))
    val s3meta = new ObjectMetadata()
    s3meta.setContentLength(metaDoc.length)

    val putRequest = new PutObjectRequest(metaBucket,destFileName,strm,s3meta)
      .withCannedAcl(CannedAccessControlList.Private)

    s3Client.putObject(putRequest)
    logger.info(s"Metadata write completed")
    Future.successful( () )
  }

  def buildGraph(implicit comm:VSCommunicator,system:ActorSystem, mat:Materializer) = {
    val counterSinkFact = Sink.fold[Int, VSLazyItem](0)((ctr,_)=>ctr+1)

    GraphDSL.create(counterSinkFact) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val src = builder.add(new VSItemSearchSource(Seq("gnm_original_filename","representativeThumbnail"),makeVSSearch.toString(),includeShape=true,pageSize=vsPageSize))
      val metadataGrabber = builder.add(new VSItemGetFullMeta(writeMetaCallback))
      val uploadProxy = builder.add(new UploadItemShape(proxyShapeNames,proxyBucket,CannedAcl.Private))
      val uploadThumb = builder.add(new UploadItemThumbnail(proxyBucket,CannedAcl.Private))
      val uploadMedia = builder.add(new UploadItemShape(Seq("original"),mediaBucket,CannedAcl.Private))
      src ~> metadataGrabber ~> uploadProxy ~> uploadThumb ~> uploadMedia ~> sink
      ClosedShape
    }
  }

  def main(args:Array[String]) = {
    //we need to disable the content length limit as we can be dealing with some VERY large files.
    val akkaConfig = ConfigFactory.parseMap(Map("akka.http.client.parsing.max-content-length"->"infinite").asJava)
    implicit val actorSystem = ActorSystem("vs-media-backup", akkaConfig)

    implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

    implicit val vsComm = new VSCommunicator(uri"$vsUrl",vsUser,vsPasswd)

    RunnableGraph.fromGraph(buildGraph).run().onComplete({
      case Success(ctr)=>
        logger.info(s"Processing completed, migrated $ctr items")
        actorSystem.terminate()
      case Failure(err)=>
        logger.error(s"Could not run migration: ", err)
        actorSystem.terminate()
    })
  }
}

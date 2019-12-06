import java.io.{ByteArrayInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{Framing, GraphDSL, Source}
import akka.util.ByteString
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{CannedAccessControlList, ObjectMetadata, PutObjectRequest}
import com.gu.vidispineakka.streamcomponents.{VSItemGetFullMeta, VSItemSearchSource}
import com.gu.vidispineakka.vidispine.{VSCommunicator, VSLazyItem}
import org.slf4j.LoggerFactory
import streamComponents.UploadItemShape

import scala.concurrent.Future

object PushProxiesMain {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val actorSystem = ActorSystem("vs-media-backup")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

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

  def buildGraph(implicit comm:VSCommunicator) = {

    GraphDSL.create() { implicit builder=>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      //empty field list => don't
      val src = builder.add(new VSItemSearchSource(Seq("gnm_original_filename","representativeThumbnail"),makeVSSearch.toString(),includeShape=true,pageSize=vsPageSize))
      val metadataGrabber = builder.add(new VSItemGetFullMeta(writeMetaCallback))
      val uploadProxy = builder.add(new UploadItemShape(proxyShapeNames,proxyBucket,CannedAcl.Private))

      src ~> metadataGrabber ~> uploadProxy
      ClosedShape
    }
  }

  def main(args:Array[String]) = {

  }
}

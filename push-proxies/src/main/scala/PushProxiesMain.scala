import java.io.{ByteArrayInputStream, File}
import java.nio.charset.StandardCharsets
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.{ActorMaterializer, ClosedShape, FlowShape, Materializer}
import akka.stream.scaladsl.{Balance, GraphDSL, Merge, RunnableGraph, Sink}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{CannedAccessControlList, ObjectMetadata, PutObjectRequest}
import com.gu.vidispineakka.streamcomponents.{VSItemGetFullMeta, VSItemSearchSource}
import com.gu.vidispineakka.vidispine.{VSCommunicator, VSLazyItem}
import helpers.CategoryPathMap
import org.slf4j.LoggerFactory
import streamComponents._

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import com.softwaremill.sttp._
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.collection.JavaConverters._

object PushProxiesMain extends FilenameHelpers {
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

  val projectBucket = sys.env.get("PROJECT_BUCKET") match {
    case Some(b)=>b
    case None=>throw new RuntimeException("You must specify PROJECT_BUCKET in the environment")
  }

  val paralellism = sys.env.getOrElse("PARALELLISM","4").toInt

  val missingFilesPath = sys.env.getOrElse("MISSING_FILES_REPORTDIR",".")
  val storagePathProperties = sys.env.get("CATEGORY_PATH_PROPERTIES")
  val maybeStoragePathMap = storagePathProperties.map(propsfile=>new CategoryPathMap(new File(propsfile)))

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
    val baseFilePath = determineFileName(forItem, forItem.shapes.flatMap(_.get("original"))) match {
      case Some(filePath)=>filePath + ".xml"
      case None=>forItem.itemId + ".xml"
    }

    val destFileName = maybeStoragePathMap.flatMap(smap=>
      forItem.getSingle("gnm_asset_category").flatMap(smap.pathPrefixForStorage)
    ) match {
      case Some(prefix)=>prefix + "/" + baseFilePath
      case None=>baseFilePath
    }

    val uploadBucket = if(forItem.getSingle("gnm_type").map(_.toLowerCase).contains("projectfile")){
      projectBucket
    } else {
      metaBucket
    }

    logger.info(s"Writing metadata to $destFileName on $uploadBucket")

    val strm = new ByteArrayInputStream(metaDoc.getBytes(StandardCharsets.UTF_8))
    val s3meta = new ObjectMetadata()
    s3meta.setContentLength(metaDoc.length)

    val putRequest = new PutObjectRequest(uploadBucket,destFileName,strm,s3meta)
      .withCannedAcl(CannedAccessControlList.Private)

    s3Client.putObject(putRequest)
    logger.info(s"Metadata write completed")
    Future.successful( () )
  }

  /**
    * builds the main, paralelling graph. This builds a parallel pipeline with the result of `buildGraphContents` in
    * `paralellism` streams
    * @param counter an optional ActorRef pointing to LostFilesCounter
    * @param maybeStoragePathMap an optional CategoryPathMap that modifies the output path depending on the category field
    * @param comm implicitly provided VSCommunicator
    * @param system implicitly provided ActorSystem
    * @param mat implicitly provided Materializer
    * @return a closed Graph
    */
  def buildGraph(counter:Option[ActorRef], maybeStoragePathMap:Option[CategoryPathMap])(implicit comm:VSCommunicator, system:ActorSystem, mat:Materializer) = {
    val counterSinkFact = Sink.fold[Int, VSLazyItem](0)((ctr,_)=>ctr+1)
    val perThreadGraph = buildGraphContents(counter, maybeStoragePathMap)

    GraphDSL.create(counterSinkFact) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new VSItemSearchSource(Seq("gnm_asset_category","gnm_original_filename","representativeThumbnail","gnm_type"),makeVSSearch.toString(),includeShape=true,pageSize=vsPageSize))
      val splitter = builder.add(Balance[VSLazyItem](paralellism,waitForAllDownstreams=true))
      val merger = builder.add(Merge[VSLazyItem](paralellism,eagerComplete=false))

      src ~> splitter

      for(i<-0 until paralellism){
        val subStream = builder.add(perThreadGraph)
        splitter.out(i) ~> subStream ~> merger.in(i)
      }

      merger ~> sink
      ClosedShape
    }
  }

  /**
    * builds a Graph to process a single thread of content
    * @param counter
    * @param maybeStoragePathMap
    * @param comm
    * @param system
    * @param mat
    * @return
    */
  private def buildGraphContents(counter:Option[ActorRef], maybeStoragePathMap:Option[CategoryPathMap])(implicit comm:VSCommunicator, system:ActorSystem, mat:Materializer) = {
    GraphDSL.create() { implicit builder=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val projectSwitch = builder.add(new IsProjectSwitch)
      val metaGrabberFactory = new VSItemGetFullMeta(writeMetaCallback)
      val mediaMetadataGrabber = builder.add(metaGrabberFactory)
      val projectMetadataGrabber = builder.add(metaGrabberFactory)

      val uploadProxy = builder.add(new UploadItemShape(proxyShapeNames,proxyBucket,CannedAcl.Private,None,maybeStoragePathMap))
      val uploadThumb = builder.add(new UploadItemThumbnail(proxyBucket,CannedAcl.Private, maybeStoragePathMap))
      val uploadMedia = builder.add(new UploadItemShape(Seq("original"),mediaBucket,CannedAcl.Private,counter, maybeStoragePathMap))
      val uploadProject = builder.add(new UploadItemShape(Seq("original"),projectBucket,CannedAcl.Private,counter, maybeStoragePathMap))
      val finalMerge = builder.add(new Merge[VSLazyItem](2, eagerComplete = false))

      //"is a project" branch
      projectSwitch.out(0) ~> projectMetadataGrabber ~> uploadProject ~> finalMerge
      //"not a project" branch
      projectSwitch.out(1) ~> mediaMetadataGrabber ~> uploadProxy ~> uploadThumb ~> uploadMedia ~> finalMerge

      FlowShape(projectSwitch.in, finalMerge.out)
    }
  }

  def main(args:Array[String]) = {
    import akka.pattern.ask
    implicit val timeout:akka.util.Timeout = 60 seconds

    //we need to disable the content length limit as we can be dealing with some VERY large files.
    val akkaConfig = ConfigFactory.parseMap(
      Map(
        "akka.http.client.parsing.max-content-length"->"infinite",
        "akka.http.host-connection-pool.response-entity-subscription-timeout"->"10.seconds",
        "akka.http.host-connection-pool.max-connections"->"8"
      ).asJava)

    implicit val actorSystem = ActorSystem("vs-media-backup", akkaConfig)
    implicit val mat:Materializer = ActorMaterializer.create(actorSystem)
    implicit val vsComm = new VSCommunicator(uri"$vsUrl",vsUser,vsPasswd)

    val lostFilesCounter = actorSystem.actorOf(Props(classOf[LostFilesCounter]))

    RunnableGraph.fromGraph(buildGraph(Some(lostFilesCounter), maybeStoragePathMap)).run().onComplete({
      case Success(ctr)=>
        val finalTs = ZonedDateTime.now().format(DateTimeFormatter.ofPattern("YYYYmmdd-HHMM"))
        logger.info(s"Outputting missing files report to lostfiles-$finalTs.csv")

        (lostFilesCounter ? LostFilesCounter.Dump(s"lostfiles-$finalTs.csv")).andThen({
          case _=>
            logger.info(s"Processing completed, migrated $ctr items")
            //actorSystem.terminate()
        })

      case Failure(err)=>
        val finalTs = ZonedDateTime.now().format(DateTimeFormatter.ofPattern("YYYYmmdd-HHMM"))
        logger.info(s"Outputting missing files report to ${missingFilesPath}/lostfiles-$finalTs.csv")
        (lostFilesCounter ? LostFilesCounter.Dump(s"${missingFilesPath}/lostfiles-$finalTs.csv")).andThen({
          case _ =>
            logger.error(s"Could not run migration: ", err)
            //actorSystem.terminate()
        })
    })
  }
}

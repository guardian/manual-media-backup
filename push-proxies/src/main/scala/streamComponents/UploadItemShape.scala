package streamComponents

import java.net.URLEncoder

import akka.actor.ActorRef
import akka.http.scaladsl.model.{ContentType, Uri}
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.stream.{Attributes, ClosedShape, FlowShape, Inlet, KillSwitches, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.util.ByteString
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.gu.vidispineakka.streamcomponents.VSFileContentSource
import com.gu.vidispineakka.vidispine.{VSCommunicator, VSFile, VSFileState, VSLazyItem, VSShape}
import helpers.CategoryPathMap
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class UploadItemShape(shapeNameAnyOf:Seq[String], bucketName:String, cannedAcl:CannedAcl, lostFilesCounter:Option[ActorRef]=None, maybeStoragePathMap:Option[CategoryPathMap]=None)(implicit comm:VSCommunicator, mat:Materializer)
  extends GraphStage[FlowShape[VSLazyItem, VSLazyItem ]] with FilenameHelpers {

  import streamComponents.LostFilesCounter._
  private final val in:Inlet[VSLazyItem] = Inlet.create("UploadItemShape.in")
  private final val out:Outlet[VSLazyItem] = Outlet.create("UploadItemShape.out")

  override def shape: FlowShape[VSLazyItem, VSLazyItem] = FlowShape.of(in, out)

  def findShape(forItem:VSLazyItem, shapeName:String):Option[VSShape] = forItem.shapes.flatMap(_.get(shapeName))

  protected val s3Client = AmazonS3ClientBuilder.defaultClient()

  /**
    * tries to get hold of an Akka source for the content of the given file
    * @param forFile file to get content for
    * @param otherFiles a sequence of VSFile objects representing duplicates of the file. If the `forFile` fails then these will
    *                   be retried in order until a valid one is found
    * @param logger implicitly provided org.slf4j.Logger
    * @return a Future containing the bytestring Source.  The Future is failed if all of the files error.
    */
  def getContentSource(forFile:VSFile, otherFiles:Seq[VSFile])(implicit logger:org.slf4j.Logger):Future[(Source[ByteString,Any],VSFile)] =
    callSourceFor(forFile).flatMap({
      case Right(source)=>Future((source,forFile))
      case Left(err)=>
        logger.warn(s"Could not get source for file ${forFile.vsid}: $err")
        if(otherFiles.nonEmpty){
          getContentSource(otherFiles.head, otherFiles.tail)
        } else {
          Future.failed(new RuntimeException("Could not get a content source for any file"))
        }
    })

  //callout to static object done this way to make mocking easier
  protected def callSourceFor(forFile:VSFile) = VSFileContentSource.sourceFor(forFile)

  /**
    * build a graph to copy from a ByteString source to an S3 file
    * @param src ByteString source
    * @param fileName file name to create remotely
    * @return
    */
  def createCopyGraph(src:Source[ByteString,Any], fileName:String, mimeType:ContentType) = {
    val sink = S3.multipartUpload(bucketName,fileName, contentType=mimeType, cannedAcl=cannedAcl)

    src.toMat(sink)(Keep.right)
  }

  def findClosedFiles(vsFiles:Seq[VSFile]):Seq[VSFile] = {
    vsFiles.filter(_.state.contains(VSFileState.CLOSED))
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private implicit val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    private var canComplete:Boolean=true

    val completedCb = createAsyncCallback[VSLazyItem](i=>{
      logger.info(s"called completedCb")
      canComplete=true
      push(out, i)
    })

    val failedCb = createAsyncCallback[Throwable](err=>{
      logger.error("Called failedCallback: ", err)
      canComplete=true
      failStage(err)
    })

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)
        canComplete = false

        val shapes = shapeNameAnyOf.map(shapeName=>
          findShape(elem,shapeName)
        ).collect({case Some(s)=>s})

        if(shapes.nonEmpty){
          if(shapes.length>1){
            logger.warn(s"Got shapes multiple shapes $shapes for item ${elem.itemId}, using the first")
          }

          val potentialFiles = findClosedFiles(shapes.head.files)
          if(potentialFiles.nonEmpty) {
            getContentSource(shapes.head.files.head, shapes.head.files.tail).flatMap(contentSource => {
              val src = contentSource._1
              val srcFile = contentSource._2

              determineFileName(elem, Some(shapes.head)) match {
                case Some(baseFilePath) =>
                  logger.info(s"Category is ${elem.getSingle("gnm_asset_category")}")
                  val filepath = maybeStoragePathMap.flatMap(smap=>
                    elem.getSingle("gnm_asset_category").flatMap(smap.pathPrefixForStorage)
                  ) match {
                    case Some(prefix)=>prefix + "/" + baseFilePath
                    case None=>baseFilePath
                  }
                  val shapeMimeTypeString = Option(shapes.head.mimeType).flatMap(str => if (str == "") None else Some(str)).getOrElse("application/octet-stream")
                  ContentType.parse(shapeMimeTypeString) match {
                    case Right(mimeType) =>
                      logger.info(s"Determined $filepath as the path to upload")
                      val fixedFileName = fixFileExtension(filepath, shapes.head.files.head)
                      logger.info(s"Filename with fixed extension is $fixedFileName")
                      if (s3Client.doesObjectExist(bucketName, fixedFileName)) {
                        logger.warn(s"File $fixedFileName already exists in $bucketName, not over-writing")
                        //set up and immediately terminate a stream in order to satisfy "Response entity not subscribed" warning
                        val ks = src.viaMat(KillSwitches.single)(Keep.right).to(Sink.ignore).run()
                        ks.shutdown()
                        Future(MultipartUploadResult(Uri(s"s3://$bucketName/${URLEncoder.encode(fixedFileName,"UTF-8")}"), bucketName, fixedFileName, "existing", None))
                      } else {
                        val graph = createCopyGraph(src, fixedFileName, mimeType)
                        RunnableGraph.fromGraph(graph).run()
                      }
                    case Left(errs) =>
                      logger.error(s"Could not determine mime type from ${shapes.head.mimeType} with ${errs.length} errors: ")
                      errs.foreach(err => logger.error(s"${err.errorHeaderName}: ${err.detail}"))
                      Future.failed(new RuntimeException(errs.head.summary))
                  }
                case None =>
                  Future.failed(new RuntimeException("Could not determine any filepath to upload for "))
              }
            }).flatMap(uploadResult => {
              logger.info(s"Uploaded to ${uploadResult.location}")
              completedCb.invokeWithFeedback(elem)
            }).recoverWith({
              case err: java.lang.RuntimeException=>
                if(err.getMessage.contains("Could not get a content source") && lostFilesCounter.isDefined) {
                  logger.error("Detected missing file from Vidispine")
                  shapes.headOption.map(topShape=> {
                    topShape.files.headOption.map(topFile=>
                    lostFilesCounter.get ! RegisterLost(topFile, topShape, elem)
                    )
                  })
                  completedCb.invokeWithFeedback(elem)
                } else {
                  logger.error(s"lostFilesCounter is ${lostFilesCounter}")
                  failedCb.invokeWithFeedback(err)
                }
              case err: Throwable =>
                logger.error(s"Could not perform upload for any of shape $shapeNameAnyOf on item ${elem.itemId}: ", err)
                failedCb.invokeWithFeedback(err)
            })
          } else {
            logger.error(s"There were no files to upload on item ${elem.itemId}")
            if(lostFilesCounter.isDefined){
              //notify the counter that we have a lost file, if there are any
              shapes.headOption.map(topShape=> {
                topShape.files.headOption.map(topFile=>
                  lostFilesCounter.get ! RegisterLost(topFile, topShape, elem)
                )
              })
            }
            completedCb.invokeWithFeedback(elem)
          }
        } else {
          val actualShapeNames = elem.shapes.map(_.keySet)
          logger.error(s"No shapes could be found matching $shapeNameAnyOf on the given item (got $actualShapeNames)")
          push(out, elem)
        }
      }

    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

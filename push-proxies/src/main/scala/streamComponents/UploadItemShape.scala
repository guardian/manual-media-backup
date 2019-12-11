package streamComponents

import akka.actor.ActorRef
import akka.http.scaladsl.model.ContentType
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{GraphDSL, Keep, RunnableGraph, Source}
import akka.stream.{Attributes, ClosedShape, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.util.ByteString
import com.gu.vidispineakka.streamcomponents.VSFileContentSource
import com.gu.vidispineakka.vidispine.{VSCommunicator, VSFile, VSFileState, VSLazyItem, VSShape}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class UploadItemShape(shapeNameAnyOf:Seq[String], bucketName:String, cannedAcl:CannedAcl, lostFilesCounter:Option[ActorRef]=None)(implicit comm:VSCommunicator, mat:Materializer)
  extends GraphStage[FlowShape[VSLazyItem, VSLazyItem ]] with FilenameHelpers {

  import streamComponents.LostFilesCounter._
  private final val in:Inlet[VSLazyItem] = Inlet.create("UploadItemShape.in")
  private final val out:Outlet[VSLazyItem] = Outlet.create("UploadItemShape.out")

  override def shape: FlowShape[VSLazyItem, VSLazyItem] = FlowShape.of(in, out)

  def findShape(forItem:VSLazyItem, shapeName:String):Option[VSShape] = forItem.shapes.flatMap(_.get(shapeName))

  /**
    * tries to get hold of an Akka source for the content of the given file
    * @param forFile file to get content for
    * @param otherFiles a sequence of VSFile objects representing duplicates of the file. If the `forFile` fails then these will
    *                   be retried in order until a valid one is found
    * @param logger implicitly provided org.slf4j.Logger
    * @return a Future containing the bytestring Source.  The Future is failed if all of the files error.
    */
  def getContentSource(forFile:VSFile, otherFiles:Seq[VSFile])(implicit logger:org.slf4j.Logger):Future[Source[ByteString,Any]] =
    callSourceFor(forFile).flatMap({
      case Right(source)=>Future(source)
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

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
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
            getContentSource(shapes.head.files.head, shapes.head.files.tail).flatMap(src =>
              determineFileName(elem, Some(shapes.head)) match {
                case Some(filepath) =>
                  val shapeMimeTypeString = Option(shapes.head.mimeType).flatMap(str => if (str == "") None else Some(str)).getOrElse("application/octet-stream")
                  ContentType.parse(shapeMimeTypeString) match {
                    case Right(mimeType) =>
                      logger.info(s"Determined $filepath as the path to upload")
                      val fixedFileName = fixFileExtension(filepath, shapes.head.files.head)
                      logger.info(s"Filename with fixed extension is $fixedFileName")
                      val graph = createCopyGraph(src, fixedFileName, mimeType)
                      RunnableGraph.fromGraph(graph).run()
                    case Left(errs) =>
                      logger.error(s"Could not determine mime type from ${shapes.head.mimeType} with ${errs.length} errors: ")
                      errs.foreach(err => logger.error(s"${err.errorHeaderName}: ${err.detail}"))
                      Future.failed(new RuntimeException(errs.head.summary))
                  }
                case None =>
                  Future.failed(new RuntimeException("Could not determine any filepath to upload for "))
              }
            ).flatMap(uploadResult => {
              logger.info(s"Uploaded to ${uploadResult.location}")
              completedCb.invokeWithFeedback(elem)
            }).recoverWith({
              case err: Throwable =>
                logger.error(s"Could not perform upload for any of shape $shapeNameAnyOf on item ${elem.itemId}: ", err)
                failedCb.invokeWithFeedback(err)
            })
          } else {
            logger.error(s"There were no files to upload on item ${elem.itemId}")
            if(lostFilesCounter.isDefined){
              //notify the counter that we have a lost file
              lostFilesCounter.get ! RegisterLost(shapes.head.files.head,shapes.head, elem)
            }
            completedCb.invokeWithFeedback(elem)
          }
        } else {
          val actualShapeNames = elem.shapes.map(_.keySet)
          logger.error(s"No shapes could be found matching $shapeNameAnyOf on the given item (got $actualShapeNames)")
          push(out, elem)
        }
      }

      //override the finish function to ensure that any async procesing has completed before we allow ourselves
      //to shut down
//      override def onUpstreamFinish(): Unit = {
//        var i=0
//
//        logger.info(s"Upstream finished")
//        while(!canComplete){
//          logger.info(s"Async processing ongoing, waiting for completion...")
//          i+=1
//          if(i>10) canComplete=true
//          Thread.sleep(1000)
//        }
//        logger.info(s"Processing completed")
//        completeStage()
//      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
//
//      override def onDownstreamFinish(): Unit = {
//        logger.info("Downstream finished")
//      }
    })
  }
}

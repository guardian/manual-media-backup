package streamComponents

import akka.http.scaladsl.model.ContentType
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{GraphDSL, Keep, RunnableGraph, Source}
import akka.stream.{Attributes, ClosedShape, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.util.ByteString
import com.gu.vidispineakka.streamcomponents.VSFileContentSource
import com.gu.vidispineakka.vidispine.{VSCommunicator, VSFile, VSLazyItem, VSShape}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class UploadItemShape(shapeNameAnyOf:Seq[String], bucketName:String, cannedAcl:CannedAcl)(implicit comm:VSCommunicator, mat:Materializer)
  extends GraphStage[FlowShape[VSLazyItem, VSLazyItem ]] with FilenameHelpers {

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
    VSFileContentSource.sourceFor(forFile).flatMap({
      case Right(source)=>Future(source)
      case Left(err)=>
        logger.warn(s"Could not get source for file ${forFile.vsid}: $err")
        if(otherFiles.nonEmpty){
          getContentSource(otherFiles.head, otherFiles.tail)
        } else {
          Future.failed(new RuntimeException("Could not get a content source for any file"))
        }
    })

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

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private implicit val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      private val completedCb = createAsyncCallback[VSLazyItem](i=>push(out, i))
      private val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

      override def onPush(): Unit = {
        val elem = grab(in)

        val shapes = shapeNameAnyOf.map(shapeName=>
          findShape(elem,shapeName)
        ).collect({case Some(s)=>s})

        if(shapes.nonEmpty){
          if(shapes.length>1){
            logger.warn(s"Got shapes multiple shapes $shapes for item ${elem.itemId}, using the first")
          }

          val result = getContentSource(shapes.head.files.head, shapes.head.files.tail).flatMap(src=>
            determineFileName(elem, Some(shapes.head)) match {
              case Some(filepath)=>
                ContentType.parse(shapes.head.mimeType) match {
                  case Right(mimeType) =>
                    logger.info(s"Determined $filepath as the path to upload")
                    val fixedFileName = fixFileExtension(filepath, shapes.head.files.head)
                    logger.info(s"Filename with fixed extension is $fixedFileName")
                    val graph = createCopyGraph(src, fixedFileName, mimeType)
                    RunnableGraph.fromGraph(graph).run()
                  case Left(errs) =>
                    logger.error(s"Could not determine mime type with ${errs.length} errors: ")
                    errs.foreach(err => logger.error(err.toString))
                    Future.failed(new RuntimeException(errs.head.toString))
                }
              case None=>
                Future.failed(new RuntimeException("Could not determine any filepath to upload for "))
            }
          )

          result.onComplete({
            case Failure(err)=>
              logger.error(s"Could not perform upload for any of shape $shapeNameAnyOf on item ${elem.itemId}: ", err)
              failedCb.invoke(err)
            case Success(uploadResult)=>
              logger.info(s"Uploaded to ${uploadResult.location}")
              completedCb.invoke(elem)
          })

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

import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{MXFSFileAttributes, MxsObject}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * look up metadata for the given objectmatrix entry
  * @param mat
  * @param ec
  */
class OMLookupMetadata(implicit mat:Materializer, ec:ExecutionContext) extends GraphStage[FlowShape[ObjectMatrixEntry,ObjectMatrixEntry]] {
  private final val in:Inlet[ObjectMatrixEntry] = Inlet.create("OMLookupMetadata.in")
  private final val out:Outlet[ObjectMatrixEntry] = Outlet.create("OMLookupMetadata.out")

  override def shape: FlowShape[ObjectMatrixEntry, ObjectMatrixEntry] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    /**
      * iterates the available metadata and presents it as a dictionary
      * @param obj [[MxsObject]] entity to retrieve information from
      * @param mat implicitly provided materializer for streams
      * @param ec implicitly provided execution context
      * @return a Future, with the relevant map
      */
    def getAttributeMetadata(obj:MxsObject)(implicit mat:Materializer, ec:ExecutionContext) = {
      val view = obj.getAttributeView

      val sink = Sink.fold[Seq[(String,AnyRef)],(String,AnyRef)](Seq())((acc,elem)=>acc++Seq(elem))

      Source.fromIterator(()=>view.iterator.asScala)
        .map(elem=>(elem.getKey, elem.getValue))
        .toMat(sink)(Keep.right)
        .run()
        .map(_.toMap)
    }

    /**
      * get the MXFS file metadata
      * @param obj [[MxsObject]] entity to retrieve information from
      * @return
      */
    def getMxfsMetadata(obj:MxsObject) = {
      val view = obj.getMXFSFileAttributeView
      view.readAttributes()
    }

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem=grab(in)

        val completeCb = getAsyncCallback[(ObjectMatrixEntry,Map[String,Any],MXFSFileAttributes)](argTuple=>{
          val updated = argTuple._1.copy(
            attributes = Some(argTuple._2),
            fileAttribues = Some(FileAttributes(argTuple._3))
          )
          push(out, updated)
        })

        val failedCb = getAsyncCallback[Throwable](err=>failStage(err))

        try {
          val vault = elem.vault
          val obj = vault.getObject(elem.oid)

          getAttributeMetadata(obj).onComplete({
            case Success(meta)=>
              completeCb.invoke((elem, meta, getMxfsMetadata(obj)))
            case Failure(exception)=>
              logger.error(s"Could not look up metadata: ", exception)
              failedCb.invoke(exception)
          })

        } catch {
          case err:Throwable=>
            logger.error(s"Could not look up object metadata: ", err)
            failStage(err)
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

package helpers

import java.nio.ByteBuffer
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.om.mxs.client.japi.MxsObject
import models.MxsMetadata
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object MetadataHelper {
  private val logger = LoggerFactory.getLogger(getClass)
  /**
    * iterates the available metadata and presents it as a dictionary
    * @param obj [[MxsObject]] entity to retrieve information from
    * @param mat implicitly provided materializer for streams
    * @param ec implicitly provided execution context
    * @return a Future, with the relevant map
    */
  def getAttributeMetadata(obj:MxsObject)(implicit mat:Materializer, ec:ExecutionContext) = {
    //val sink = Sink.fold[Seq[(String,AnyRef)],(String,AnyRef)](Seq())((acc,elem)=>acc++Seq(elem))
    val sink = Sink.fold[MxsMetadata,(String,Any)](MxsMetadata.empty())((acc,elem)=>{
      elem._2 match {
        case null=>acc
        case boolValue: Boolean => acc.copy(boolValues = acc.boolValues ++ Map(elem._1->boolValue))
        case intValue:Int => acc.copy(intValues = acc.intValues ++ Map(elem._1 -> intValue))
        case longValue:Long => acc.copy(longValues = acc.longValues ++ Map(elem._1 -> longValue))
        case byteBuffer:ByteBuffer => acc.copy(stringValues = acc.stringValues ++ Map(elem._1 -> Hex.encodeHexString(byteBuffer.array())))
        case floatValue:java.lang.Float => acc.copy(floatValues = acc.floatValues ++ Map(elem._1 -> floatValue.toFloat))
        case stringValue:String => acc.copy(stringValues = acc.stringValues ++ Map(elem._1 -> stringValue))
        case _=>
          try {
            logger.warn(s"Could not get metadata value for ${elem._1} on ${obj.getId}, type ${elem._2.getClass.toString} not recognised")
          } catch {
            case err:Throwable=>
              logger.warn(s"Could not get metadata value for ${elem._1}: ", err)
          }
          acc
      }
    })

    Option(obj.getAttributeView) match {
      case Some(view) =>
        Source.fromIterator(() => view.iterator.asScala)
          .map(elem => (elem.getKey, elem.getValue))
          .toMat(sink)(Keep.right)
          .run()
      case None=>
        Future(MxsMetadata.empty)
    }
  }

  def getAttributeMetadataSync(obj:MxsObject) = {
    val view = obj.getAttributeView

    view.iterator.asScala.foldLeft(MxsMetadata.empty){ (acc, elem)=>{
      val v = elem.getValue.asInstanceOf[Any]
      v match {
        case boolValue: Boolean => acc.copy(boolValues = acc.boolValues ++ Map(elem.getKey->boolValue))
        case intValue:Int => acc.copy(intValues = acc.intValues ++ Map(elem.getKey -> intValue))
        case longValue:Long => acc.copy(longValues = acc.longValues ++ Map(elem.getKey -> longValue))
        case byteBuffer:ByteBuffer => acc.copy(stringValues = acc.stringValues ++ Map(elem.getKey -> Hex.encodeHexString(byteBuffer.array())))
        case stringValue:String => acc.copy(stringValues = acc.stringValues ++ Map(elem.getKey -> stringValue))
        case _=>
          logger.warn(s"Could not get metadata value for ${elem.getKey} on ${obj.getId}, type ${elem.getValue.getClass.toString} not recognised")
          acc
      }
    }}
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

  def setAttributeMetadata(obj:MxsObject, newMetadata:MxsMetadata, attempt:Int=0):Try[Unit] = {
    def internalSetAttributeMetadata = Try {
      import scala.collection.JavaConverters._
      val view = obj.getAttributeView

      view.writeAllAttributes(newMetadata.toAttributes(filterUnwritable = true).asJavaCollection)
    }

    internalSetAttributeMetadata match {
      case result@Success(_)=>
        result
      case problem@Failure(err)=>
        if(err.getMessage.contains("error 311")) {
          logger.debug(err.getMessage)
          logger.warn(s"Got error string containing `error 311` on attempt $attempt, assuming locking timeout. Retrying in 5s...")
          Thread.sleep(5000)
          setAttributeMetadata(obj, newMetadata, attempt+1)
        } else {
          logger.error(s"Could not set metadata: ${err.getMessage}", err)
          problem
        }
    }
  }
}

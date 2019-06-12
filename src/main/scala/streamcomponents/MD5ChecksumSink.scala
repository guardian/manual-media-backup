package streamcomponents

import java.security.MessageDigest

import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{AbstractInHandler, GraphStageLogic, GraphStageWithMaterializedValue}
import akka.util.ByteString
import org.slf4j.LoggerFactory
import org.apache.commons.codec.binary.Hex

import scala.concurrent.{Future, Promise}

class MD5ChecksumSink extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[String]] {
  private final val in:Inlet[ByteString] = Inlet.create("MD5ChecksumSink.in")

  override def shape: SinkShape[ByteString] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[String]) = {
    val completionPromise = Promise[String]()

    val logic = new GraphStageLogic(shape) {
      private val logger = LoggerFactory.getLogger(getClass)
      private val md5Instance = MessageDigest.getInstance("md5")


      setHandler(in, new AbstractInHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          md5Instance.update(elem.toArray)
          pull(in)
        }
      })

      override def preStart(): Unit = pull(in)

      override def postStop(): Unit = {
        val str = Hex.encodeHexString(md5Instance.digest())
        completionPromise.success(str)
      }
    }
    (logic, completionPromise.future)
  }
}

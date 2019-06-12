package streamcomponents

import java.io.InputStream
import java.nio.{ByteBuffer, DirectByteBuffer}

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.util.ByteString
import com.om.mxs.client.japi.{AccessOption, MatrixStore, MxsObject, SeekableByteChannel, UserInfo, Vault}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


class MatrixStoreFileSource(mxsFile:MxsObject, bufferSize:Int=2*1024*1024) extends GraphStage[SourceShape[ByteString]]{
  private final val out:Outlet[ByteString] = Outlet.create("MatrixStoreFileSource.out")

  override def shape: SourceShape[ByteString] = SourceShape.of(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)
    private var channel:SeekableByteChannel = _

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = {
        val buffer = ByteBuffer.allocate(bufferSize)
        val bytesRead = channel.read(buffer)

        if(bytesRead == -1){
          logger.info(s"MXS file read on ${mxsFile.getId} completed")
          complete(out)
        } else {
          logger.debug(s"Pushing $bytesRead bytes into the stream...")
          val bytes = new Array[Byte](bytesRead)
          buffer.get(bytes)
          push(out,ByteString(bytes))
        }
      }
    })

    override def preStart(): Unit = {
      channel = mxsFile.newSeekableObjectChannel(Set(AccessOption.READ).asJava)
    }

    override def postStop(): Unit = {
      if(channel!=null) channel.close()
    }
  }
}

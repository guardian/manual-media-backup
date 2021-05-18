package streamcomponents

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Keep, Sink}
import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.FileEntry
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory

import java.nio.file.Path
import java.security.MessageDigest
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class LocalChecksum(implicit actorySystem:ActorSystem, mat:Materializer) extends GraphStage[FlowShape[FileEntry, FileEntry]] {
  private final val logger = LoggerFactory.getLogger(getClass)
  private final val in:Inlet[FileEntry] = Inlet("LocalChecksum.in")
  private final val out:Outlet[FileEntry] = Outlet("LocalChecksum.out")
  private implicit val ec:ExecutionContext = actorySystem.dispatcher

  override def shape: FlowShape[FileEntry, FileEntry] = FlowShape.of(in, out)

  /**
    * asynchronously performs a digest of the given file, using akka io
    * @param forFile java.nio.Path pointing to the file to checksum
    * @return a Future containing the hex string of the checksum, which fails if there is a problem.
    */
  def performChecksumming(forFile:Path) = {
    val md = MessageDigest.getInstance("md5")

    FileIO.fromPath(forFile)
      .map(bytes=>md.update(bytes.toArray))
      .toMat(Sink.ignore)(Keep.right)
      .run()
      .map(_=>md.digest())
      .map(csBytes=>Hex.encodeHexString(csBytes))
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var isInProgress = false
    private var isCompleting = false

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })

    val completionCb = createAsyncCallback[FileEntry](entry=>{
      push(out, entry)
      isInProgress = false
      if(isCompleting) completeStage()
    })

    val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        isInProgress = true
        performChecksumming(elem.localFile.filePath).onComplete({
          case Success(checksum)=>
            logger.info(s"Checksummed ${elem.localFile.filePath.toString} successfully: $checksum")
            val updatedLocalFile = elem.localFile.copy(md5=Some(checksum))
            completionCb.invoke(elem.copy(localFile = updatedLocalFile))
          case Failure(err)=>
            logger.error(s"Could not perform checksumming on ${elem.localFile.filePath.toString}: ${err.getMessage}", err)
            isInProgress = false
            failedCb.invoke(err)
        })
      }

      override def onUpstreamFinish(): Unit = {
        //normally, if the upstream completes then we do too. But if we are waiting for an async operation, then we
        //can only complete once it has finished. We therefore record the fact that we are about to complete here,
        //and the actual completion is carried out in the `completionCb` above.
        if(isInProgress) {
          isCompleting = true
        } else {
          completeStage()
        }
      }
    })
  }
}

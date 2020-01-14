package streamcomponents

import akka.actor.ActorSystem
import akka.stream.scaladsl.RunnableGraph
import akka.stream.{Attributes, ClosedShape, FlowShape, Graph, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{MatrixStore, MxsObject, UserInfo, Vault}
import helpers.{Copier, MatrixStoreHelper}
import models.{BackupEntry, BackupStatus}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class BatchCopyFile (userInfo:UserInfo, checksumType: String, chunkSize:Int, maxTries:Int=10)(implicit system:ActorSystem, mat:Materializer) extends GraphStage[FlowShape[BackupEntry, BackupEntry]] {
  private final val in:Inlet[BackupEntry] = Inlet.create("BatchCopyFile.in")
  private final val out:Outlet[BackupEntry] = Outlet.create("BatchCopyFile.out")

  override def shape: FlowShape[BackupEntry, BackupEntry] = FlowShape.of(in,out)

  /**
    * calls out to an external graph to perform the file copy and validates the checksums.
    * It retries recursively, up to the `maxTries` parameter on the parent class
    * @param copyGraph akka Graph to perform the copy. It must be a ClosedShape and materialize a Future of Option of String that contains the checksum as calculated during copy
    * @param mxsObject `MxsObject` representing the target file
    * @param attempt attempt counter, defaults to 1, don't include this when calling
    * @param logger implicitly provided Logger instance
    * @return a Future containing the appliance checksum.  The Future fails on error, use .onComplete or .recover to detect this
    */
  def makeCopy(copyGraph:Graph[ClosedShape.type, Future[Option[String]]], mxsObject: MxsObject, checksumType:String, attempt:Int=1)(implicit logger:org.slf4j.Logger):Future[String] = {
    if(attempt>maxTries) return Future.failed(new RuntimeException(s"Gave up after ${attempt-1} tries"))

    val checksumFut = RunnableGraph.fromGraph(copyGraph).run().flatMap(maybeCopiedChecksum=>{
      if(checksumType=="none"){
        Future(None)
      } else {
        MatrixStoreHelper.getOMFileMd5(mxsObject).map({
          case Success(applianceChecksum) => Some((maybeCopiedChecksum, applianceChecksum))
          case Failure(err) => throw err
        })
      }
    })

    checksumFut.flatMap({
      case Some((Some(copiedChecksum), applianceChecksum)) =>
        if (copiedChecksum != applianceChecksum) {
          logger.error(s"Copied checksum did not match appliance; appliance gave $applianceChecksum and copy gave $copiedChecksum. Assuming that the file is corrupted.")
          makeCopy(copyGraph, mxsObject, checksumType, attempt + 1)
        } else {
          logger.info(s"Checksums matched")
          Future(applianceChecksum)
        }
      case Some((None, applianceChecksum)) =>
        logger.warn("Can't validate checksum as no checksum was requested")
        Future(applianceChecksum)
      case None =>
        logger.warn("Can't validate checksum as no checksum was requested")
        Future("")
      })
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private implicit val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    private var maybeVault:Option[Vault] = None

    val completedCb = createAsyncCallback[BackupEntry](e=>push(out, e))
    val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val fileToBackUp = elem.originalPath.toFile
        val maybeMxsObject = elem.maybeObjectMatrixEntry.map(ent=>Try {maybeVault.get.getObject(ent.oid)})

        maybeMxsObject match {
          case None=>
            logger.error(s"Incoming element for ${elem.originalPath.toString} has no mxs file!")
            failStage(new RuntimeException("No incoming mxs file"))
          case Some(Failure(err))=>
            logger.error(s"Could not get existing MxsEntry from the appliance: ", err)
            failStage(err)
          case Some(Success(mxsObject))=>
            val copyGraph = Copier.createCopyGraph(fileToBackUp, chunkSize*1024, checksumType, mxsObject)
            makeCopy(copyGraph, mxsObject, checksumType).onComplete({
              case Success(applianceChecksum)=> //if we get here either the checksums matched or no checksumming was requested
                elem.maybeObjectMatrixEntry.get.attributes
                val updated = elem.copy(applianceChecksum=Some(applianceChecksum), status = BackupStatus.BACKED_UP)
                completedCb.invoke(updated)
              case Failure(err)=>
                logger.error(s"File copy of ${elem.originalPath.toString} failed: ", err)
                logger.warn(s"Deleting partial or corrupted file for ${elem.originalPath.toString} on destination")
                mxsObject.delete()
                failedCb.invoke(err)
            })
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })

    override def preStart(): Unit = {
      try {
        maybeVault = Some(MatrixStore.openVault(userInfo))
      } catch {
        case err:Throwable=>
          logger.error("Can't open vault: ", err)
          failStage(err)
      }
    }

    override def postStop(): Unit = {
      maybeVault.map(_.dispose())
    }
  }
}

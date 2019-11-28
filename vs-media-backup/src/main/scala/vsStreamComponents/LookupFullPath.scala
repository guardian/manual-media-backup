package vsStreamComponents

import java.io.File

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.VSBackupEntry
import org.slf4j.LoggerFactory
import vidispine.{VSCommunicator, VSStorage}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class LookupFullPath (comm:VSCommunicator)(implicit mat:Materializer) extends GraphStage[FlowShape[VSBackupEntry,VSBackupEntry ]]{
  private final val in:Inlet[VSBackupEntry] = Inlet.create("LookupFullPath.in")
  private final val out:Outlet[VSBackupEntry] = Outlet.create("LookupFullPath.out")

  override def shape: FlowShape[VSBackupEntry, VSBackupEntry] = FlowShape.of(in,out)

  def lookupStorageInfo(storageId:String) = {
    comm.requestGet(s"/API/storage/$storageId",headers = Map("Accept"->"application/xml")).map(_.map(VSStorage.fromXmlString))
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    private var storageCache:Map[String,VSStorage] = Map()

    def getStorageInfoFromCache(storageId:String) = {
      storageCache.get(storageId) match {
        case Some(storageInfo)=>Future(Right(storageInfo))
        case None=>
          lookupStorageInfo(storageId).map({
            case Left(err)=>
              logger.error(s"Could not look up storage info: $err")
              Left(err.toString)
            case Right(Failure(parseError))=>
              logger.error(s"Got data from Vidispine but could not parse the response: ", parseError)
              Left(parseError.toString)
            case Right(Success(storageInfo))=>
              logger.info(s"Successfully looked up storage $storageId")
              storageCache = storageCache ++ Map(storageId->storageInfo)
              Right(storageInfo)
          })
      }
    }

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val successCb = createAsyncCallback[VSBackupEntry](entry=>push(out,entry))
        val errorCb = createAsyncCallback[Throwable](err=>fail(out, err))
        val skipCb = createAsyncCallback[Unit](_=>pull(in))

        getStorageInfoFromCache(elem.sourceStorage.get).map({
          case Left(err)=>
            logger.error(s"Could not get source storage information: $err")
            errorCb.invoke(new RuntimeException(err))
          case Right(storageInfo)=>
            if(storageInfo.fileSystemPaths.headOption.isEmpty){
              logger.error("Source storage has no local filesystem path! Can't continue.")
              skipCb.invoke(())
            } else {
              val sourcePath = "/" + storageInfo.fileSystemPaths.head + elem.storageSubpath.get
              logger.info(s"Got local source path $sourcePath")
              val f = new File(sourcePath)
              if(!f.exists()){
                logger.error(s"File $sourcePath does not exist on source storage, can't continue.")
                skipCb.invoke(())
              } else {
                val updatedElem = elem.copy(fullPath = Some(sourcePath))
                successCb.invoke(updatedElem)
              }
            }
        })
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

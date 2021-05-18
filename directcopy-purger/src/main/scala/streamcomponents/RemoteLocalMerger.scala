package streamcomponents

import akka.stream.{Attributes, Inlet, Outlet, UniformFanInShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.FileEntry
import org.slf4j.LoggerFactory

import scala.collection.mutable

class RemoteLocalMerger extends GraphStage[UniformFanInShape[FileEntry, FileEntry]] {
  private final val logger = LoggerFactory.getLogger(getClass)
  private final val localInlet:Inlet[FileEntry] = Inlet.create("RemoteLocalMerger.localIn")
  private final val remoteInlet:Inlet[FileEntry] = Inlet.create("RemoteLocalMerger.remoteIn")
  private final val out:Outlet[FileEntry] = Outlet.create("RemoteLocalMerger.out")

  override def shape: UniformFanInShape[FileEntry, FileEntry] = new UniformFanInShape[FileEntry, FileEntry](out, Array(localInlet, remoteInlet))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var completedUpstreams = 0

    private val waitingFromRemote:mutable.Map[String, FileEntry] = mutable.Map()
    private val waitingFromLocal:mutable.Map[String, FileEntry] = mutable.Map()

    def completeStageIfRequired(): Unit = {
      if(completedUpstreams==2) {
        logger.info("All upstreams have now completed, finishing")
        completeStage()
      }
    }

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = {
        if(!hasBeenPulled(localInlet) && !isClosed(localInlet)) pull(localInlet)
        if(!hasBeenPulled(remoteInlet) && !isClosed(remoteInlet)) pull(remoteInlet)
      }
    })

    setHandler(localInlet, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(localInlet)

        waitingFromRemote.get(elem.localFile.filePath.toString) match {
          case Some(remoteWaiting)=>  //there is something matching this file already been delivered from the remote side. Put them together and send it on.
            val finalElem = elem.copy(remoteFile = remoteWaiting.remoteFile)
            push(out, finalElem)
            waitingFromRemote.remove(elem.localFile.filePath.toString)
          case None=>                 //nothing corresponding to this file has come through from the remote yet. Put it on the queue and request the next item.
            waitingFromLocal(elem.localFile.filePath.toString) = elem
            pull(localInlet)
        }
      }

      override def onUpstreamFinish(): Unit = {
        logger.info("locals upstream completed")
        completedUpstreams+=1
        completeStageIfRequired()
      }
    })

    setHandler(remoteInlet, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(remoteInlet)

        waitingFromLocal.get(elem.localFile.filePath.toString) match {
          case Some(localWaiting)=>
            val finalElem = elem.copy(localFile = localWaiting.localFile)
            push(out, finalElem)
            waitingFromLocal.remove(elem.localFile.filePath.toString)
          case None=>
            waitingFromRemote(elem.localFile.filePath.toString) = elem
            pull(remoteInlet)
        }
      }

      override def onUpstreamFinish(): Unit = {
        logger.info("remote upstream completed")
        completedUpstreams+=1
        completeStageIfRequired()
      }
    })

    override def postStop(): Unit = {
      if(waitingFromLocal.nonEmpty) {
        logger.warn(s"Run completed with ${waitingFromLocal.count(_=>true)} local files not found on remote")
        var i=0
        waitingFromLocal.foreach(entry=>{
          i+=1
          logger.info(s"\tLocal file $i not matched to remote: ${entry._1}")
        })
      }
      if(waitingFromRemote.nonEmpty) {
        logger.warn(s"Run completed with ${waitingFromRemote.count(_=>true)} remote files not found on local")
        var i=0
        waitingFromRemote.foreach(entry=>{
          i+=1
          logger.info(s"\tRemote file $i not matched to remote: ${entry._1}")
        })
      }
    }
  }
}

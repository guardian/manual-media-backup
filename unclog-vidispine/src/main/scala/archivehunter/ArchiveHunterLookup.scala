package archivehunter

import java.net.URI

import akka.actor.ActorSystem
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.stream._
import models.PotentialArchiveTarget
import org.slf4j.LoggerFactory
import vidispine.VSFile

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

/**
  * graph stage to look up the provided entry in Archive Hunter
  * @param baseUri base URL at which ArchiveHunter is available
  * @param key shared secret key that allows server->server communication with AH
  * @param system implicitly provided Actor System
  * @param mat implicitly provided Materializer
  */
class ArchiveHunterLookup(baseUri:String, key:String)(implicit val system:ActorSystem, implicit val mat:Materializer) extends GraphStage[FanOutShape2[PotentialArchiveTarget, PotentialArchiveTarget, PotentialArchiveTarget ]]{
  private final val in:Inlet[PotentialArchiveTarget] = Inlet.create("ArchiveHunterLookup.in")
  private final val yes:Outlet[PotentialArchiveTarget] = Outlet.create("ArchiveHunterLookup.vsFileOut")
  private final val no:Outlet[PotentialArchiveTarget] = Outlet.create("ArchiveHunterLookup.ahNearlineOut")

  override def shape: FanOutShape2[PotentialArchiveTarget, PotentialArchiveTarget, PotentialArchiveTarget ] = new FanOutShape2(in, yes, no)

  protected val requestor = new ArchiveHunterRequestor(baseUri, key)

  def extractPathPart(ommsUrl:URI) = {
    val pathParts = ommsUrl.getPath.split("/")
    pathParts.drop(3).mkString("/")
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    private var canTerminate = true
    private var mustTerminate = false

    val yesCb = createAsyncCallback[PotentialArchiveTarget](entry=>{
      canTerminate=true
      push(yes, entry)
      if(mustTerminate){
        logger.debug("stage completing in response to previous upstream completion")
        completeStage()
      }
    })

    val noCb = createAsyncCallback[PotentialArchiveTarget](entry=>{
      canTerminate=true
      push(no, entry)
      if(mustTerminate){
        logger.debug("stage completing in response to previous upstream completion")
        completeStage()
      }
    })

    val failedCb = createAsyncCallback[Throwable](err=>{
      canTerminate=true
      failStage(err)
    })

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        logger.debug(s"ArchiveHunterLookup - checking ${elem.mxsFilename}")

        canTerminate = false
        requestor.lookupRequest(elem.mxsFilename).onComplete({
          case Failure(err)=>
            logger.error("ArchiveHunter lookup crashed: ", err)
            failedCb.invoke(err)
          case Success(Left(err))=>
            logger.error(s"ArchiveHunter returned an error: $err")
            failedCb.invoke(new RuntimeException("ArchiveHunter returned an error. Consult logs for details."))
          case Success(Right(ArchiveHunterNotFound))=>
            logger.info(s"Item is not present in ArchiveHunter")
            noCb.invoke(elem)
          case Success(Right(found:ArchiveHunterFound))=>
            logger.info(s"Item found in archivehunter at ${found.archiveHunterCollection} with ID ${found.archiveHunterId}")
            if(found.beenDeleted){
              logger.warn(s"Item ${elem.mxsFilename} found in ArchiveHunter but has been deleted! Assuming it does not exist")
              noCb.invoke(elem)
            } else {
              yesCb.invoke(elem.copy(archivedSize = Some(found.size)))
            }
        })
      }

      override def onUpstreamFinish(): Unit = {
        if(canTerminate) {
          completeStage()
        } else {
          logger.debug("Upstream terminated but we are still waiting for an async operation")
          mustTerminate = true
        }
      }
    })

    val genericOutHandler = new AbstractOutHandler {
      override def onPull(): Unit = if(!hasBeenPulled(in)) pull(in)
    }

    setHandler(yes, genericOutHandler)
    setHandler(no, genericOutHandler)
  }
}

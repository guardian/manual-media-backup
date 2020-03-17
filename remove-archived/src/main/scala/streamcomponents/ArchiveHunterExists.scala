package streamcomponents

import akka.actor.ActorSystem
import akka.stream.{Attributes, Inlet, Materializer, Outlet, UniformFanOutShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import helpers.{ArchiveHunterFound, ArchiveHunterNotFound, ArchiveHunterRequestor}
import models.PotentialRemoveStreamObject
import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * pushes the incoming element to "YES" if a file exists in ArchiveHunter, "NO" otherwise
  */
class ArchiveHunterExists(baseUri:String, key:String, stripPathElements:Int)(implicit val system:ActorSystem, implicit val mat:Materializer) extends GraphStage[UniformFanOutShape[PotentialRemoveStreamObject, PotentialRemoveStreamObject ]] {
  private final val in:Inlet[PotentialRemoveStreamObject] = Inlet.create("ArchiveHunterExists.in")
  private final val yes:Outlet[PotentialRemoveStreamObject] = Outlet.create("ArchiveHunterExists.yes")
  private final val no:Outlet[PotentialRemoveStreamObject] = Outlet.create("ArchiveHunterExists.no")

  override def shape: UniformFanOutShape[PotentialRemoveStreamObject, PotentialRemoveStreamObject] = new UniformFanOutShape[PotentialRemoveStreamObject, PotentialRemoveStreamObject](in, Array(yes,no))

  lazy private val requestor = new ArchiveHunterRequestor(baseUri, key)

  def stripPath(incomingPath:String)(implicit logger:Logger):String = {
    if(stripPathElements==0){
      incomingPath
    } else {
      val parts = incomingPath.split("/")
      if(parts.length<stripPathElements) {
        logger.warn(s"path was too short to split! Length was ${parts.length} and want to split $stripPathElements")
        incomingPath
      } else {
        parts.slice(stripPathElements,parts.length).mkString("/")
      }
    }
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private implicit val logger = LoggerFactory.getLogger(getClass)

    private var canTerminate = true
    private var mustTerminate = false

    setHandler(in, new AbstractInHandler {
      private val yesCb = createAsyncCallback[PotentialRemoveStreamObject](elem=>{
        canTerminate=true
        push(yes,elem)
        if(mustTerminate) completeStage()
      })
      private val noCb = createAsyncCallback[PotentialRemoveStreamObject](elem=>{
        canTerminate=true
        push(no,elem)
        if(mustTerminate) completeStage()
      })
      private val errCb = createAsyncCallback[Throwable](err=>{
        canTerminate=true
        failStage(err)
        if(mustTerminate) completeStage()
      })

      override def onPush(): Unit = {
        val elem = grab(in)

        elem.omFile.pathOrFilename match {
          case None=>
            logger.warn(s"${elem.omFile.oid} does not have a filename!")
            push(no, elem)
          case Some(filename)=>
            logger.debug(s"ArchiveHunterExists - checking $filename")
            canTerminate = false
            requestor.lookupRequest(stripPath(filename)).onComplete({
              case Failure(err)=>
                logger.error("ArchiveHunter lookup crashed: ", err)
                errCb.invoke(err)
              case Success(Left(err))=>
                logger.error(s"ArchiveHunter returned an error: $err")
                errCb.invoke(new RuntimeException("ArchiveHunter returned an error. Consult logs for details."))
              case Success(Right(ArchiveHunterNotFound))=>
                logger.info(s"$filename does not exist in archive")
                noCb.invoke(elem)
              case Success(Right(found:ArchiveHunterFound))=>
                logger.info(s"$filename found in ArchiveHunter at ${found.archiveHunterCollection} with ID ${found.archiveHunterId}")
                yesCb.invoke(elem.copy(archivedSize = found.size))
            })
        }
      }

      override def onUpstreamFinish(): Unit = {
        if(canTerminate){
          logger.debug("upstream terminated and we can too")
          completeStage()
        } else {
          logger.debug("upstream terminated but we are still waiting for an async response")
          mustTerminate=true
        }
      }
    })

    private val genericOutHandler = new AbstractOutHandler {
      override def onPull(): Unit = if(!hasBeenPulled(in)) pull(in)
    }

    setHandler(yes, genericOutHandler)
    setHandler(no, genericOutHandler)
  }
}

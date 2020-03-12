package streamcomponents

import akka.stream.{Attributes, Inlet, Outlet, UniformFanOutShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import helpers.{ArchiveHunterFound, ArchiveHunterNotFound}
import models.{ObjectMatrixEntry, PotentialRemoveStreamObject}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
  * checks whether the given file's local and remote sizes match. If so the element is pushed to YES, if not then it is pushed to NO
  */
class ArchiveHunterFileSizeSwitch extends GraphStage[UniformFanOutShape[PotentialRemoveStreamObject, PotentialRemoveStreamObject]] {
  private val in:Inlet[PotentialRemoveStreamObject] = Inlet.create("ArchiveHunterFileSizeSwitch.in")
  private val yes:Outlet[PotentialRemoveStreamObject] = Outlet.create("ArchiveHunterFileSizeSwitch.yes")
  private val no:Outlet[PotentialRemoveStreamObject] = Outlet.create("ArchiveHunterFileSizeSwitch.no")

  override def shape: UniformFanOutShape[PotentialRemoveStreamObject, PotentialRemoveStreamObject] = new UniformFanOutShape(in, Array(yes,no))

  def getMaybeLocalSize(entry:ObjectMatrixEntry):Option[Long] = {
    entry.longAttribute("DPSP_SIZE") match {
      case value @Some(_)=>value
      case None=>
        entry.stringAttribute("DPSP_SIZE").map(_.toLong)
    }
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {

      override def onPush(): Unit = {
        val elem = grab(in)

        elem.archivedSize match {
          case None=>
            logger.warn(s"${elem.omFile.pathOrFilename.getOrElse(elem.omFile.oid)} has no archived size!")
            push(no, elem)
          case Some(archivedSize)=>
            getMaybeLocalSize(elem.omFile) match {
              case Some(localSize) =>
                if (archivedSize == localSize) {
                  logger.debug(s"${elem.omFile.pathOrFilename.getOrElse(elem.omFile.oid)} sizes match")
                  push(yes, elem)
                } else {
                  logger.debug(s"${elem.omFile.pathOrFilename.getOrElse(elem.omFile.oid)} sizes differ")
                  push(no, elem)
                }
              case None=>
                logger.warn(s"${elem.omFile.pathOrFilename.getOrElse(elem.omFile.oid)} has no nearline size!")
                push(no, elem)
            }
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

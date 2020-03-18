package streamcomponents

import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.stream.{Attributes, Inlet, Outlet, UniformFanOutShape}
import models.{ObjectMatrixEntry, PotentialArchiveTarget}
import org.slf4j.LoggerFactory

/**
  * checks whether the given file's local and remote sizes match. If so the element is pushed to YES, if not then it is pushed to NO
  */
class ArchiveHunterFileSizeSwitch extends GraphStage[UniformFanOutShape[PotentialArchiveTarget, PotentialArchiveTarget]] {
  private val in:Inlet[PotentialArchiveTarget] = Inlet.create("ArchiveHunterFileSizeSwitch.in")
  private val yes:Outlet[PotentialArchiveTarget] = Outlet.create("ArchiveHunterFileSizeSwitch.yes")
  private val no:Outlet[PotentialArchiveTarget] = Outlet.create("ArchiveHunterFileSizeSwitch.no")

  override def shape: UniformFanOutShape[PotentialArchiveTarget, PotentialArchiveTarget] = new UniformFanOutShape(in, Array(yes,no))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {

      override def onPush(): Unit = {
        val elem = grab(in)

        (elem.archivedSize, elem.byteSize) match {
          case (None,None)=>
            logger.warn(s"${elem.mxsFilename} has no archived nor local size!")
            push(no, elem)
          case (Some(_),None)=>
            logger.warn(s"${elem.mxsFilename} has no local size")
            push(no, elem)
          case (None, Some(_))=>
            logger.warn(s"${elem.mxsFilename} has no archived size")
            push(no, elem)
          case (Some(archivedSize), Some(localSize))=>
            if (archivedSize == localSize) {
              logger.debug(s"${elem.mxsFilename} sizes match")
              push(yes, elem)
            } else {
              logger.debug(s"${elem.mxsFilename} sizes differ")
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

package streamcomponents

import akka.stream.{Attributes, Inlet, Outlet, UniformFanOutShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.PotentialRemoveStreamObject
import org.slf4j.LoggerFactory
import java.io.File

/**
  * pushes the incoming element to "YES" if a file exists in the same location in the local filesystem, otherwise NO
  */
class LocalFileExists extends GraphStage[UniformFanOutShape[PotentialRemoveStreamObject, PotentialRemoveStreamObject ]] {
  private final val in:Inlet[PotentialRemoveStreamObject] = Inlet.create("LocalFileExists.in")
  private final val yes:Outlet[PotentialRemoveStreamObject] = Outlet.create("LocalFileExists.yes")
  private final val no:Outlet[PotentialRemoveStreamObject] = Outlet.create("LocalFileExists.no")

  override def shape: UniformFanOutShape[PotentialRemoveStreamObject, PotentialRemoveStreamObject] = new UniformFanOutShape(in,Array(yes,no))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        elem.omFile.pathOrFilename match {
          case None=>
            logger.warn(s"OM file ${elem.omFile.oid} has no path!")
            pull(in)
          case Some(filepath)=>
            logger.debug(s"checking filepath $filepath")
            val f = new File(filepath)
            if(f.exists()) {
              push(yes, elem)
              logger.debug(s"$filepath exists")
            } else {
              push(no, elem)
              logger.debug(s"$filepath does not exist")
            }
        }
      }
    })

    setHandler(yes, new AbstractOutHandler {
      override def onPull(): Unit = if(!hasBeenPulled(in)) pull(in)
    })

    setHandler(no, new AbstractOutHandler {
      override def onPull(): Unit = if(!hasBeenPulled(in)) pull(in)
    })
  }
}

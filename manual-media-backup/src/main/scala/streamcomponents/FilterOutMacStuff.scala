package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import org.slf4j.LoggerFactory
import java.nio.file.Path

/**
  * filters out mac-specific files that start with ._
  */
class FilterOutMacStuff extends GraphStage[FlowShape[Path,Path]]{
  private final val in:Inlet[Path] = Inlet.create("FilterOutMacStuff.in")
  private final val out:Outlet[Path] = Outlet.create("FilterOutMacStuff.out")

  override def shape: FlowShape[Path, Path] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        if(elem.getFileName.toString.startsWith("._")){
          pull(in)
        } else {
          push(out, elem)
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

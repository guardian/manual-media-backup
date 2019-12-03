package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import org.slf4j.LoggerFactory

import java.nio.file.Path

class FilterOutDirectories extends GraphStage[FlowShape[Path, Path]] {
  private final val in:Inlet[Path] = Inlet.create("FilterOutDirectories.in")
  private final val out:Outlet[Path] = Outlet.create("FilterOutDirectories.out")

  override def shape: FlowShape[Path, Path] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val path = grab(in)

        if(path.toFile.isDirectory){
          logger.debug(s"File ${path.toString()} is a directory, ignoring")
          pull(in)
        } else {
          push(out, path)
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

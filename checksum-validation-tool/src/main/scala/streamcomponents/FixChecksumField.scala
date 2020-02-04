package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.ObjectMatrixEntry

/**
  * removes - from the __mxs__calc_md5 field
  */
class FixChecksumField extends GraphStage[FlowShape[ObjectMatrixEntry, ObjectMatrixEntry]]{
  private final val in:Inlet[ObjectMatrixEntry] = Inlet.create("FixChecksumField.in")
  private final val out:Outlet[ObjectMatrixEntry] = Outlet.create("FixChecksumField.out")

  override def shape = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val maybeValue = elem.attributes.flatMap(_.stringValues.get("__mxs__calc_md5"))
        val maybeUpdatedValue = maybeValue.map(prevValue=>prevValue.replace("-",""))

        if(maybeUpdatedValue.isDefined) {
          val updatedMap = elem.attributes.get.stringValues + ("__mxs__calc_md5"->maybeUpdatedValue.get)
          val updatedAttribs = elem.attributes.get.copy(stringValues = updatedMap)
          val updatedElem = elem.copy(attributes = Some(updatedAttribs))
          push(out, updatedElem)
        } else {
          push(out,elem)
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

package streamComponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet, UniformFanOutShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import com.gu.vidispineakka.vidispine.VSLazyItem
import org.slf4j.LoggerFactory

class IsProjectSwitch extends GraphStage[UniformFanOutShape[VSLazyItem,VSLazyItem]] {
  private final val in:Inlet[VSLazyItem] = Inlet.create("IsProjectSwitch.in")
  private final val yes:Outlet[VSLazyItem] = Outlet.create("IsProjectSwitch.yes")
  private final val no:Outlet[VSLazyItem] = Outlet.create("IsProjectSwitch.no")

  override def shape: UniformFanOutShape[VSLazyItem, VSLazyItem] = new UniformFanOutShape[VSLazyItem, VSLazyItem](in,Array(yes,no))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val item = grab(in)

        item.getSingle("gnm_type") match {
          case None=>
            logger.warn(s"gnm_type is not set on item ${item.itemId}")
            push(no, item)
          case Some(pt)=>
            logger.debug(s"gnm_type is $pt")
            if(pt.toLowerCase=="projectfile"){
              push(yes, item)
            } else {
              push(no,item)
            }
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

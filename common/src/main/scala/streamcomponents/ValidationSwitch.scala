package streamcomponents

import akka.stream.{Attributes, FanOutShape, Inlet, Outlet, UniformFanOutShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.{CopyReport, ObjectMatrixEntry}
import org.slf4j.LoggerFactory

//class CopyReportSplitterShape extends FanOutShape[CopyReport] {
//
//  override def construct(init: FanOutShape.Init[CopyReport]): FanOutShape[CopyReport] = ???
//  newOutlet[CopyReport]("yes")
//  newOutlet[ObjectMatrixEntry]("no")
//}
///**
//  * "switch" stage to divert failed CopyReports
//  */
//class ValidationSwitch extends GraphStage[FanOutShape[CopyReport]]{
//  private final val in:Inlet[CopyReport] = Inlet.create("ValidationSwitch.in")
//  private final val outYes:Outlet[CopyReport] = Outlet.create("ValidationSwitch.yes")
//  private final val outNo:Outlet[ObjectMatrixEntry] = Outlet.create("ValidationSwitch.no")
//
//  override def shape: UniformFanOutShape[CopyReport, ObjectMatrixEntry] = UniformFanOutShape(in, outYes, outNo)
//
//  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
//    private val logger = LoggerFactory.getLogger(getClass)
//
//    setHandler(in, new AbstractInHandler {
//      override def onPush(): Unit = {
//        val elem = grab(in)
//
//        elem.validationPassed match {
//          case Some(true)=>push(outYes, elem)
//          case Some(false)=>push(outNo, elem)
//          case None=>push(outYes, elem)
//        }
//      }
//    })
//
//    setHandler(outYes, new AbstractOutHandler {
//      override def onPull(): Unit = if(isAvailable(in)) pull(in)
//    })
//
//    setHandler(outNo, new AbstractOutHandler {
//      override def onPull(): Unit = if(isAvailable(in)) pull(in)
//    })
//  }
//}

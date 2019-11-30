package streamcomponents

import akka.stream.{Attributes, InPort, Inlet, Outlet, Shape}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic, GraphStageWithMaterializedValue}
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.util.Success

class TwoPortCounterShape[T](ins:immutable.Seq[Inlet[T]]) extends Shape {
  override def inlets: immutable.Seq[Inlet[T]] = ins

  override def outlets: immutable.Seq[Outlet[_]] = immutable.Seq()

  override def deepCopy(): Shape = {
    val inletCopies = ins.map(_.carbonCopy())
    new TwoPortCounterShape[T](inletCopies)
  }
}

case class CounterData(count1:Int,count2:Int)

/**
  * this class implements a "sink-like" counter that has two seperate inputs.
  * it counts how many entries go to each input and materializes that value as a Future at the end (as
  * an instance of the case class CounterData as above)
  * @tparam T type of data that the inputs will receive
  */
class TwoPortCounter[T] extends GraphStageWithMaterializedValue[TwoPortCounterShape[T], Future[CounterData]]{
  private final val in1:Inlet[T] = Inlet.create("TwoPortCounter.in1")
  private final val in2:Inlet[T] = Inlet.create("TwoPortCounter.in2")

  override def shape: TwoPortCounterShape[T] = new TwoPortCounterShape(immutable.Seq(in1,in2))

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[CounterData]) = {
    val counterPromise = Promise[CounterData]
    var counter = CounterData(0,0)

    var in1Completed:Boolean = false
    var in2Completed:Boolean = false

    val logic = new GraphStageLogic(shape) {
      private val logger = LoggerFactory.getLogger(getClass)

      setHandler(in1, new AbstractInHandler {
        override def onPush(): Unit = {
          val elem = grab(in1)
          println(s"Got $elem")
          counter = counter.copy(count1=counter.count1+1)
          pull(in1)
        }

        override def onUpstreamFinish(): Unit = {
          in1Completed = true
          if(in1Completed && in2Completed) completeStage()
        }
      })

      setHandler(in2, new AbstractInHandler {
        override def onPush(): Unit = {
          counter = counter.copy(count2=counter.count2+1)
          pull(in2)
        }

        override def onUpstreamFinish(): Unit = {
          in2Completed = true
          if(in1Completed && in2Completed) completeStage()
        }
      })

      override def preStart(): Unit = {
        pull(in1)
        pull(in2)
      }

      override def postStop(): Unit = {
        counterPromise.complete(Success(counter))
      }
    }

    (logic, counterPromise.future)
  }
}

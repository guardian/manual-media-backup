package streamcomponents

import akka.stream.{Attributes, InPort, Inlet, Outlet, Shape}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic, GraphStageWithMaterializedValue}
import org.slf4j.LoggerFactory

import scala.collection.{immutable, mutable}
import scala.concurrent.{Future, Promise}
import scala.util.Success

class MultiPortCounterShape[T](ins:immutable.Seq[Inlet[T]]) extends Shape {
  override def inlets: immutable.Seq[Inlet[T]] = ins

  override def outlets: immutable.Seq[Outlet[_]] = immutable.Seq()

  override def deepCopy(): Shape = {
    val inletCopies = ins.map(_.carbonCopy())
    new MultiPortCounterShape[T](inletCopies)
  }
}

case class CounterData(counts:Map[Int,Int]) {
  def count1 = counts.getOrElse(0,0)
  def count2 = counts.getOrElse(1,0)

  def update(channel:Int,value:Int=1) = {
    val updatedCounter = this.counts.get(channel) match {
      case None=>
        this.counts ++ Map(channel->value)
      case Some(existingCount)=>
        this.counts ++ Map(channel->(existingCount+value))
    }
    this.copy(counts=updatedCounter)
  }
}

/**
  * this class implements a "sink-like" counter that has two seperate inputs.
  * it counts how many entries go to each input and materializes that value as a Future at the end (as
  * an instance of the case class CounterData as above)
  * @tparam T type of data that the inputs will receive
  */
class MultiPortCounter[T](inletCount:Int) extends GraphStageWithMaterializedValue[MultiPortCounterShape[T], Promise[CounterData]]{
  /**
    * recursively generate `inletCount` inlets of type T
    * call this with no parameters to perform setup
    * @param currentSeq current value, defaults to empty
    * @param ctr iteration counter, defaults to 0
    * @return an immutable sequence of Inlet of type T
    */
  def makeInlets(currentSeq:immutable.Seq[Inlet[T]]=immutable.Seq(),ctr:Int=0):immutable.Seq[Inlet[T]] = {
    if(ctr==inletCount) return currentSeq
    val updatedSeq = currentSeq :+ Inlet.create(s"MultiPortCounter.in$ctr")
    makeInlets(updatedSeq,ctr+1)
  }

  /**
    * recursively generate a Map with one entry for each channel and a boolean completion flag
    * call this with no parameters to perform setup
    * @param currentValue current value of the map, defaults to empty
    * @param ctr iteration counter, defaults to 0
    * @return an initialised map, with `inletCount` entries keyed to the iteration count and a value of false  for each
    */
  def makeCompletionStatusMap(currentValue:mutable.Map[Int,Boolean]=mutable.Map(),ctr:Int=0):mutable.Map[Int,Boolean] = {
    if(ctr==inletCount) return currentValue
    val updated = currentValue ++ Map(ctr -> false)
    makeCompletionStatusMap(updated, ctr+1)
  }

  private final val inlets:immutable.Seq[Inlet[T]] = makeInlets()

  override def shape: MultiPortCounterShape[T] = new MultiPortCounterShape(inlets)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Promise[CounterData]) = {
    val counterPromise = Promise[CounterData]
    var counter = CounterData(Map())

    val logic = new GraphStageLogic(shape) {
      private val logger = LoggerFactory.getLogger(getClass)
      var completionStatus:mutable.Map[Int,Boolean] = makeCompletionStatusMap()

      /**
        * returns a boolean flag indicating whether all of the upstreams have completed yet
        * @return True if all upstreams have created, otherwise false
        */
      def didAllComplete = {
        !completionStatus.exists(_._2==false)
      }

      /**
        * build inlet handlers
        * @param channel channel that this inlet handler is for
        * @return an inlet handler object that can be passed to `setHandler`
        */
      def inHandlerForChannel(channel:Int) = new AbstractInHandler {
        override def onPush(): Unit = {
          grab(inlets(channel))

          counter = counter.update(channel)
          pull(inlets(channel))
        }

        override def onUpstreamFinish(): Unit = {
          completionStatus(channel) = true
          if(didAllComplete) completeStage()
        }
      }

      for(i <- 0 until inletCount){
        setHandler(inlets(i), inHandlerForChannel(i))
      }

      override def preStart(): Unit = {
        for(i<-0 until inletCount) pull(inlets(i))
      }

      override def postStop(): Unit = {
        counterPromise.complete(Success(counter))
      }
    }

    (logic, counterPromise)
  }
}

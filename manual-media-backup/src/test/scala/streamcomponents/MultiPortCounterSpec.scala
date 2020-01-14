package streamcomponents

import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, Merge, RunnableGraph, Source}
import org.specs2.mutable.Specification

import scala.concurrent.Await
import scala.concurrent.duration._

class MultiPortCounterSpec extends Specification {
  "TwoPortCounter" should {
    "Count incoming elements on both ports and materialize the final value" in new AkkaTestkitSpecs2Support {
      val sinkFact = new MultiPortCounter[Int](2)

      implicit val mat:Materializer = ActorMaterializer.create(system)
      val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src1 = Source.fromIterator(()=>Seq(1,2,3,4,5,6).toIterator)
        val src2 = Source.fromIterator(()=>Seq(1,2,3,4).toIterator)
        src1 ~> sink.inlets(0)
        src2 ~> sink.inlets(1)

        ClosedShape
      }
      Merge

      val result = Await.result(RunnableGraph.fromGraph(graph).run().future, 10 seconds)
      result mustEqual CounterData(Map(0->6,1->4))
    }
  }
}

package vsStreamComponents

import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import models.{ArchiveTargetStatus, HttpError, PotentialArchiveTarget}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import vidispine.VSCommunicator

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class VSDeleteShapeAndOrFileSpec extends Specification with Mockito {
  "VSDeleteShapeAndOrFile" should {
    "make concurrent shape and file deletion requests" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      implicit val mockedCommunicator = mock[VSCommunicator]
      mockedCommunicator.request(any,any,any,any,any,any)(any,any) returns Future(Right("mock result"))

      val sinkFact = Sink.seq[PotentialArchiveTarget]
      val graph = GraphDSL.create(sinkFact) {implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(Source.single(PotentialArchiveTarget(None,None,"fake-oid","fake-filename","VX-123",Some("VX-456"),Some(Seq("VX-567","VX-678")))))
        val toTest = builder.add(new VSDeleteShapeAndOrFile())
        src ~> toTest ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 10 seconds)

      result.length mustEqual 1
      result.head.status mustEqual ArchiveTargetStatus.SUCCESS

      there was atLeastOne(mockedCommunicator).request(VSCommunicator.OperationType.DELETE,
        "/API/storage/file/VX-123",
        None,
        Map()
      ) andThen atLeastOne (mockedCommunicator).request(VSCommunicator.OperationType.DELETE,
        "/API/item/VX-456/shape/VX-567",
        None,
        Map()
      ) andThen atLeastOne (mockedCommunicator).request(VSCommunicator.OperationType.DELETE,
      "/API/item/VX-456/shape/VX-678",
        None,
        Map()
      )
    }

    "terminate the stream if the shape deletions fail" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      implicit val mockedCommunicator = mock[VSCommunicator]
      mockedCommunicator.request(any,any,any,any,any,any)(any,any) returns Future(Left(mock[HttpError]))

      val sinkFact = Sink.seq[PotentialArchiveTarget]
      val graph = GraphDSL.create(sinkFact) {implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(Source.single(PotentialArchiveTarget(None,None,"fake-oid","fake-filename","VX-123",Some("VX-456"),Some(Seq("VX-567","VX-678")))))
        val toTest = builder.add(new VSDeleteShapeAndOrFile())
        src ~> toTest ~> sink
        ClosedShape
      }

      def theTest = {
        val result = Await.result(RunnableGraph.fromGraph(graph).run(), 10 seconds)
      }

      theTest must throwA[RuntimeException]

      there were two (mockedCommunicator).request(VSCommunicator.OperationType.DELETE,
        "/API/item/VX-456/shape/VX-567",
        None,
        Map()
      ) andThen atLeastOne (mockedCommunicator).request(VSCommunicator.OperationType.DELETE,
        "/API/item/VX-456/shape/VX-678",
        None,
        Map()
      )

    }
  }
}

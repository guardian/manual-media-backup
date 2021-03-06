import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, Merge, RunnableGraph, Sink, Source}
import models.CopyReport
import org.specs2.mutable.Specification
import streamcomponents.ValidationSwitch

import scala.concurrent.Await
import scala.concurrent.duration._

class ValidationSwitchSpec extends Specification {
  "ValidationSwitch" should {
    "push a report indicating success to YES" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val sinkFactory = Sink.fold[Seq[String],String](Seq())((acc,entry)=>acc++Seq(entry))

      val testStream = GraphDSL.create(sinkFactory) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(Source.single(CopyReport[Nothing]("some-filepath","1233456",Some("checksum"),12345L,preExisting = false,validationPassed = Some(true))))
        val switch = builder.add(new ValidationSwitch(treatNoneAsSuccess = false, treatPreExistingAsSuccess = false))
        val merge = builder.add(Merge[String](2))
        src ~> switch
        switch.out(0).map(rpt=>"YES") ~> merge //YES branch
        switch.out(1).map(rpt=>"NO") ~> merge
        merge ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(testStream).run(), 30 seconds)
      result mustEqual Seq("YES")
    }

    "push a report indicating failure to NO" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val sinkFactory = Sink.fold[Seq[String],String](Seq())((acc,entry)=>acc++Seq(entry))

      val testStream = GraphDSL.create(sinkFactory) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(Source.single(CopyReport[Nothing]("some-filepath","1233456",Some("checksum"),12345L,preExisting = false,validationPassed = Some(false))))
        val switch = builder.add(new ValidationSwitch(treatNoneAsSuccess = false, treatPreExistingAsSuccess = false))
        val merge = builder.add(Merge[String](2))
        src ~> switch
        switch.out(0).map(rpt=>"YES") ~> merge //YES branch
        switch.out(1).map(rpt=>"NO") ~> merge
        merge ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(testStream).run(), 30 seconds)
      result mustEqual Seq("NO")
    }

    "push a report without indication to YES if treatNoneAsSuccess is true" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val sinkFactory = Sink.fold[Seq[String],String](Seq())((acc,entry)=>acc++Seq(entry))

      val testStream = GraphDSL.create(sinkFactory) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(Source.single(CopyReport[Nothing]("some-filepath","1233456",Some("checksum"),12345L,preExisting = false,validationPassed = None)))
        val switch = builder.add(new ValidationSwitch(treatNoneAsSuccess = true, treatPreExistingAsSuccess = false))
        val merge = builder.add(Merge[String](2))
        src ~> switch
        switch.out(0).map(rpt=>"YES") ~> merge //YES branch
        switch.out(1).map(rpt=>"NO") ~> merge
        merge ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(testStream).run(), 30 seconds)
      result mustEqual Seq("YES")
    }

    "push a report without indication to NO if treatNoneAsSuccess is false" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val sinkFactory = Sink.fold[Seq[String],String](Seq())((acc,entry)=>acc++Seq(entry))

      val testStream = GraphDSL.create(sinkFactory) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(Source.single(CopyReport[Nothing]("some-filepath","1233456",Some("checksum"),12345L,preExisting = false,validationPassed =None)))
        val switch = builder.add(new ValidationSwitch(treatNoneAsSuccess = false, treatPreExistingAsSuccess = false))
        val merge = builder.add(Merge[String](2))
        src ~> switch
        switch.out(0).map(rpt=>"YES") ~> merge //YES branch
        switch.out(1).map(rpt=>"NO") ~> merge
        merge ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(testStream).run(), 30 seconds)
      result mustEqual Seq("NO")
    }

    "push a report indicating pre-existing to YES always if treatPreExistingAsSuccess is true" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val sinkFactory = Sink.fold[Seq[String],String](Seq())((acc,entry)=>acc++Seq(entry))

      val testStream = GraphDSL.create(sinkFactory) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(Source.single(CopyReport[Nothing]("some-filepath","1233456",None,12345L,preExisting = true,validationPassed = Some(false))))
        val switch = builder.add(new ValidationSwitch(treatNoneAsSuccess = false))
        val merge = builder.add(Merge[String](2))
        src ~> switch
        switch.out(0).map(rpt=>"YES") ~> merge //YES branch
        switch.out(1).map(rpt=>"NO") ~> merge
        merge ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(testStream).run(), 30 seconds)
      result mustEqual Seq("YES")
    }
  }


}

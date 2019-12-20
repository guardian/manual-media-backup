package streamcomponents

import java.io.{File, FileOutputStream}
import java.nio.file.Path

import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import org.specs2.mutable.Specification
import akka.stream.scaladsl.GraphDSL.Implicits._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._

class ExcludeListSwitchSpec extends Specification {
  def makeTempFile(content:String) = {
    val f = File.createTempFile("mmb-exclude-list-switch-spec", "")
    f.deleteOnExit()
    val s = new FileOutputStream(f)
    try {
      s.write(content.getBytes("UTF-8"))
      f
    } finally {
      s.close()
    }
  }

  "ExcludeListSwitch" should {
    "filter out any paths that match the given regex list" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val tempFile = makeTempFile("""["^path/to/dir1","path/to/dir2"]""")
      val listToCheck = Seq(
        "path/to/dir1/some/file",
        "/path/to/dir1/some/otherfile",
        "path/to/dir2/some/file",
        "/another/path/to/dir2/some/otherfile"
      )

      val sinkFact = Sink.seq[Path]
      val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
        val src = Source.fromIterator(()=>listToCheck.toIterator).map(p=>new File(p).toPath)
        val toTest = builder.add(new ExcludeListSwitch(Some(tempFile.getAbsolutePath)))
        src ~> toTest ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 5 seconds)
      result.length mustEqual 1
      result.head.toString mustEqual "/path/to/dir1/some/otherfile"
    }

    "pass everything through if the list is None" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val listToCheck = Seq(
        "path/to/dir1/some/file",
        "/path/to/dir1/some/otherfile",
        "path/to/dir2/some/file",
        "/another/path/to/dir2/some/otherfile"
      )

      val sinkFact = Sink.seq[Path]
      val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
        val src = Source.fromIterator(()=>listToCheck.toIterator).map(p=>new File(p).toPath)
        val toTest = builder.add(new ExcludeListSwitch(None))
        src ~> toTest ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 5 seconds)
      result.length mustEqual 4
      result.map(_.toString).toList mustEqual listToCheck.toList
    }

    "fail to start if the json list is not valid" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val tempFile = makeTempFile("""[not valid Json!]""")
      val listToCheck = Seq(
        "path/to/dir1/some/file",
        "/path/to/dir1/some/otherfile",
        "path/to/dir2/some/file",
        "/another/path/to/dir2/some/otherfile"
      )

      val sinkFact = Sink.seq[Path]
      val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
        val src = Source.fromIterator(()=>listToCheck.toIterator).map(p=>new File(p).toPath)
        val toTest = builder.add(new ExcludeListSwitch(Some(tempFile.getAbsolutePath)))
        src ~> toTest ~> sink
        ClosedShape
      }

      def theTest = {
        Await.result(RunnableGraph.fromGraph(graph).run(), 5 seconds)
      }
      theTest should throwA[RuntimeException]
    }

    "fail to start if the json is not valid" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val tempFile = makeTempFile("""[""")
      val listToCheck = Seq(
        "path/to/dir1/some/file",
        "/path/to/dir1/some/otherfile",
        "path/to/dir2/some/file",
        "/another/path/to/dir2/some/otherfile"
      )

      val sinkFact = Sink.seq[Path]
      val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
        val src = Source.fromIterator(()=>listToCheck.toIterator).map(p=>new File(p).toPath)
        val toTest = builder.add(new ExcludeListSwitch(Some(tempFile.getAbsolutePath)))
        src ~> toTest ~> sink
        ClosedShape
      }

      def theTest = {
        Await.result(RunnableGraph.fromGraph(graph).run(), 5 seconds)
      }
      theTest should throwA[RuntimeException]
    }

    "fail to start if the json is not a list" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val tempFile = makeTempFile("""{"key":"value"}""")
      val listToCheck = Seq(
        "path/to/dir1/some/file",
        "/path/to/dir1/some/otherfile",
        "path/to/dir2/some/file",
        "/another/path/to/dir2/some/otherfile"
      )

      val sinkFact = Sink.seq[Path]
      val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
        val src = Source.fromIterator(()=>listToCheck.toIterator).map(p=>new File(p).toPath)
        val toTest = builder.add(new ExcludeListSwitch(Some(tempFile.getAbsolutePath)))
        src ~> toTest ~> sink
        ClosedShape
      }

      def theTest = {
        Await.result(RunnableGraph.fromGraph(graph).run(), 5 seconds)
      }
      theTest should throwA[RuntimeException]
    }
  }
}

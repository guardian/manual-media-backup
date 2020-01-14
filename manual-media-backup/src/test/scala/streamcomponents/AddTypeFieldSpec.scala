package streamcomponents
import java.io.{File, PrintWriter}

import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.{GraphDSL, Sink, Source}
import io.circe
import models.{BackupEntry, MxsMetadata, ObjectMatrixEntry}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import akka.stream.scaladsl.GraphDSL.Implicits._

import scala.concurrent.Await
import scala.util.matching.Regex
import scala.concurrent.duration._

class AddTypeFieldSpec extends Specification with Mockito {
  "AddTypeField" should {
    "set the GNM_TYPE tag to a provided value from the config if part of the path matches" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)
      val sampleConfig = Map(
        "^/path/to/media".r -> "media",
        "^/path/to/masters".r -> "masters"
      )

      val toTest = new AddTypeField("fakefile.json") {
        override def loadKnownPaths(): Either[circe.Error, Map[Regex, String]] = Right(sampleConfig)
      }

      val testElementMetadata = MxsMetadata.empty()
      val testElementOMEntry = ObjectMatrixEntry("fake-oid",Some(testElementMetadata),None)
      val fp="/path/to/media/project/somefile.wav"
      val file = new File(fp).toPath
      val testElement = BackupEntry(file,Some(testElementOMEntry))

      val sinkFact = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
        val src = builder.add(Source.single(testElement))
        val addTypeField = builder.add(toTest)

        src ~> addTypeField ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      val outputElement = result.head
      outputElement.originalPath mustEqual testElement.originalPath
      outputElement.maybeObjectMatrixEntry must beSome
      outputElement.maybeObjectMatrixEntry.get.attributes must beSome
      val attribs = outputElement.maybeObjectMatrixEntry.get.attributes.get
      attribs.stringValues.get("GNM_TYPE") must beSome("media")
    }

    "set the GNM_TYPE tag to 'unsorted' no part of the path matches" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)
      val sampleConfig = Map(
        "^/path/to/media".r -> "media",
        "^/path/to/masters".r -> "masters"
      )

      val toTest = new AddTypeField("fakefile.json") {
        override def loadKnownPaths(): Either[circe.Error, Map[Regex, String]] = Right(sampleConfig)
      }

      val testElementMetadata = MxsMetadata.empty()
      val testElementOMEntry = ObjectMatrixEntry("fake-oid",Some(testElementMetadata),None)
      val fp="/path/to/somewhereelse/thing/stuff.doc"
      val file = new File(fp).toPath
      val testElement = BackupEntry(file,Some(testElementOMEntry))

      val sinkFact = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
        val src = builder.add(Source.single(testElement))
        val addTypeField = builder.add(toTest)

        src ~> addTypeField ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      val outputElement = result.head
      outputElement.originalPath mustEqual testElement.originalPath
      outputElement.maybeObjectMatrixEntry must beSome
      outputElement.maybeObjectMatrixEntry.get.attributes must beSome
      val attribs = outputElement.maybeObjectMatrixEntry.get.attributes.get
      attribs.stringValues.get("GNM_TYPE") must beSome("unsorted")
    }
  }

  "AddTypeField.loadKnownPaths" should {
    "read json from a given filename and output it as a domain object" in {
      val sampleConfigText = """{"^path/to/media":"media","^/path/to/masters":"masters"}"""

      val tempFi = File.createTempFile("addTypeFieldTest-",".json")
      tempFi.deleteOnExit()
      new PrintWriter(tempFi) {
        try {
          write(sampleConfigText)
        } finally {
          close()
        }
      }

      val toTest = new AddTypeField(tempFi.getAbsolutePath)
      val result = toTest.loadKnownPaths()
      result must beRight
      val map = result.right.get
      map.head._1.regex mustEqual "^path/to/media"
      map.head._2 mustEqual "media"
      map.last._1.regex mustEqual "^/path/to/masters"
      map.last._2 mustEqual "masters"
    }

    "return a left if the file could not be parsed" in {
      val sampleConfigText = """{"^path/to/media":"media,"^/path/to/masters":"masters"}"""

      val tempFi = File.createTempFile("addTypeFieldTest-",".json")
      tempFi.deleteOnExit()
      new PrintWriter(tempFi) {
        try {
          write(sampleConfigText)
        } finally {
          close()
        }
      }

      val toTest = new AddTypeField(tempFi.getAbsolutePath)
      val result = toTest.loadKnownPaths()
      result must beLeft
    }

    "return a left if the file could not be opened" in {
      val toTest = new AddTypeField("dsfsdfsahjkg")
      val result = toTest.loadKnownPaths()
      result must beLeft
    }
  }
}

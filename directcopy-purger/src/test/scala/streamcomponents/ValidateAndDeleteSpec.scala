package streamcomponents

import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

import java.nio.file.{Path, Paths}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Success, Try}
import akka.stream.scaladsl.GraphDSL.Implicits._
import models.{FileEntry, LocalFile, RemoteFile}

class ValidateAndDeleteSpec extends Specification with Mockito with AfterAll {
  implicit val actorSystem = ActorSystem()
  implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  override def afterAll(): Unit = {
    Await.ready(actorSystem.terminate(), 10.seconds)
  }

  "ValidateAndDelete" should {
    "try to delete a file if the size and checksum match" in {
      val mockDelete = mock[(Path)=>Try[Unit]]
      mockDelete.apply(any) returns Success()
      val toTest = new ValidateAndDelete(true) {
        override def doDelete(filePath: Path): Try[Unit] = mockDelete(filePath)
      }

      val sampleData = FileEntry(
        LocalFile(Paths.get("/path/to/some/file"), 1234, Some("abcde")),
        Some(RemoteFile("adhgadjhda", 1234, Some("abcde")))
      )

      val graph = GraphDSL.create(toTest) { implicit builder=> sink=>
        val src = builder.add(Source.single(sampleData))

        src ~> sink
        ClosedShape
      }

      Await.ready(RunnableGraph.fromGraph(graph).run(), 5.seconds)
      there was one(mockDelete).apply(Paths.get("/path/to/some/file"))
    }
  }

  "not try to delete a file if the size and checksum match but reallyDelete is false" in {
    val mockDelete = mock[(Path)=>Try[Unit]]
    mockDelete.apply(any) returns Success()
    val toTest = new ValidateAndDelete(false) {
      override def doDelete(filePath: Path): Try[Unit] = mockDelete(filePath)
    }

    val sampleData = FileEntry(
      LocalFile(Paths.get("/path/to/some/file"), 1234, Some("abcde")),
      Some(RemoteFile("adhgadjhda", 1234, Some("abcde")))
    )

    val graph = GraphDSL.create(toTest) { implicit builder=> sink=>
      val src = builder.add(Source.single(sampleData))

      src ~> sink
      ClosedShape
    }

    Await.ready(RunnableGraph.fromGraph(graph).run(), 5.seconds)
    there was no(mockDelete).apply(any)
  }

  "not try to delete a file if the size matches but the checksum doesn't" in {
    val mockDelete = mock[(Path)=>Try[Unit]]
    mockDelete.apply(any) returns Success()
    val toTest = new ValidateAndDelete(true) {
      override def doDelete(filePath: Path): Try[Unit] = mockDelete(filePath)
    }

    val sampleData = FileEntry(
      LocalFile(Paths.get("/path/to/some/file"), 1234, Some("abcde")),
      Some(RemoteFile("adhgadjhda", 1234, Some("abce")))
    )

    val graph = GraphDSL.create(toTest) { implicit builder=> sink=>
      val src = builder.add(Source.single(sampleData))

      src ~> sink
      ClosedShape
    }

    Await.ready(RunnableGraph.fromGraph(graph).run(), 5.seconds)
    there was no(mockDelete).apply(any)
  }

  "not try to delete a file if the size matches but a checksum is missing" in {
    val mockDelete = mock[(Path)=>Try[Unit]]
    mockDelete.apply(any) returns Success()
    val toTest = new ValidateAndDelete(true) {
      override def doDelete(filePath: Path): Try[Unit] = mockDelete(filePath)
    }

    val sampleData = FileEntry(
      LocalFile(Paths.get("/path/to/some/file"), 1234, Some("abcde")),
      Some(RemoteFile("adhgadjhda", 1234, None))
    )

    val graph = GraphDSL.create(toTest) { implicit builder=> sink=>
      val src = builder.add(Source.single(sampleData))

      src ~> sink
      ClosedShape
    }

    Await.ready(RunnableGraph.fromGraph(graph).run(), 5.seconds)
    there was no(mockDelete).apply(any)
  }

  "not try to delete a file if the sizes don't match but the checksum does" in {
    val mockDelete = mock[(Path)=>Try[Unit]]
    mockDelete.apply(any) returns Success()
    val toTest = new ValidateAndDelete(true) {
      override def doDelete(filePath: Path): Try[Unit] = mockDelete(filePath)
    }

    val sampleData = FileEntry(
      LocalFile(Paths.get("/path/to/some/file"), 1234, Some("abcde")),
      Some(RemoteFile("adhgadjhda", 12, Some("abcde")))
    )

    val graph = GraphDSL.create(toTest) { implicit builder=> sink=>
      val src = builder.add(Source.single(sampleData))

      src ~> sink
      ClosedShape
    }

    Await.ready(RunnableGraph.fromGraph(graph).run(), 5.seconds)
    there was no(mockDelete).apply(any)
  }

  "not try to delete a file and terminate with error if there is no remote file" in {
    val mockDelete = mock[(Path)=>Try[Unit]]
    mockDelete.apply(any) returns Success()
    val toTest = new ValidateAndDelete(true) {
      override def doDelete(filePath: Path): Try[Unit] = mockDelete(filePath)
    }

    val sampleData = FileEntry(
      LocalFile(Paths.get("/path/to/some/file"), 1234, Some("abcde")),
      None
    )

    val graph = GraphDSL.create(toTest) { implicit builder=> sink=>
      val src = builder.add(Source.single(sampleData))

      src ~> sink
      ClosedShape
    }

    val result = Try { Await.result(RunnableGraph.fromGraph(graph).run(), 5.seconds) }
    there was no(mockDelete).apply(any)
    result must beFailedTry
    result.failed.get must beAnInstanceOf[RuntimeException]
  }
}

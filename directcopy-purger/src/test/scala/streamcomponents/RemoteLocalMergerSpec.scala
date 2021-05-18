package streamcomponents

import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import models.{FileEntry, LocalFile, RemoteFile}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

import java.nio.file.Paths
import scala.concurrent.Await
import scala.concurrent.duration._

class RemoteLocalMergerSpec extends Specification with Mockito with AfterAll {
  implicit val system = ActorSystem()
  implicit val mat:Materializer = ActorMaterializer.create(system)

  override def afterAll() = {
    Await.result(system.terminate(), 10.seconds)
  }

  def fakeLocal(pathstr:String, length:Long) = LocalFile(Paths.get(pathstr), length, Some("local-checksum"))

  def findEntry(pathstr:String, haystack:Seq[FileEntry]) = haystack.find(_.localFile.filePath.toString==pathstr)

  "RemoteLocalMerger" should {
    "bundle together matching file entries" in {
      val fromLocalList = Seq(
        FileEntry(fakeLocal("/path/to/file1",123), None),
        FileEntry(fakeLocal("/path/to/file2",234), None),
        FileEntry(fakeLocal("/path/to/file3",345), None),
      )

      val fromRemoteList = Seq(
        FileEntry(fakeLocal("/path/to/file3",-1), Some(RemoteFile("oid-3",345,Some("remote-checksum")))),
        FileEntry(fakeLocal("/path/to/file1",-1), Some(RemoteFile("oid-1",123,Some("remote-checksum")))),
        FileEntry(fakeLocal("/path/to/file2",-1), Some(RemoteFile("oid-2",234,Some("remote-checksum")))),
      )

      val graph = GraphDSL.create(Sink.seq[FileEntry]) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val locals = builder.add(Source.fromIterator(()=>fromLocalList.iterator))
        val remotes = builder.add(Source.fromIterator(()=>fromRemoteList.iterator))
        val merger = builder.add(new RemoteLocalMerger)

        locals ~> merger.in(0)
        remotes ~> merger.in(1)
        merger.out ~> sink

        ClosedShape
      }

      val results = Await.result(RunnableGraph.fromGraph(graph).run(), 5.seconds)
      results.length mustEqual(3)

      findEntry("/path/to/file1", results) must beSome(
        FileEntry(
          LocalFile(Paths.get("/path/to/file1"), 123, Some("local-checksum")),
          Some(RemoteFile("oid-1",123,Some("remote-checksum")))
        )
      )
      findEntry("/path/to/file2", results) must beSome(
        FileEntry(
          LocalFile(Paths.get("/path/to/file2"), 234, Some("local-checksum")),
          Some(RemoteFile("oid-2",234,Some("remote-checksum")))
        )
      )
      findEntry("/path/to/file3", results) must beSome(
        FileEntry(
          LocalFile(Paths.get("/path/to/file3"), 345, Some("local-checksum")),
          Some(RemoteFile("oid-3",345,Some("remote-checksum")))
        )
      )
    }

    "not attempt to bundle together mismatched entries" in {
      val fromLocalList = Seq(
        FileEntry(fakeLocal("/path/to/file1",123), None),
        FileEntry(fakeLocal("/path/to/file2",234), None),
        FileEntry(fakeLocal("/path/to/file3",345), None),
        FileEntry(fakeLocal("/path/to/file4",456), None),
      )

      val fromRemoteList = Seq(
        FileEntry(fakeLocal("/path/to/file3",-1), Some(RemoteFile("oid-3",345,Some("remote-checksum")))),
        FileEntry(fakeLocal("/path/to/file1",-1), Some(RemoteFile("oid-1",123,Some("remote-checksum")))),
        FileEntry(fakeLocal("/path/to/file2",-1), Some(RemoteFile("oid-2",234,Some("remote-checksum")))),
        FileEntry(fakeLocal("/path/to/file5",-1), Some(RemoteFile("oid-5",567,Some("remote-checksum")))),
      )

      val graph = GraphDSL.create(Sink.seq[FileEntry]) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val locals = builder.add(Source.fromIterator(()=>fromLocalList.iterator))
        val remotes = builder.add(Source.fromIterator(()=>fromRemoteList.iterator))
        val merger = builder.add(new RemoteLocalMerger)

        locals ~> merger.in(0)
        remotes ~> merger.in(1)
        merger.out ~> sink

        ClosedShape
      }

      val results = Await.result(RunnableGraph.fromGraph(graph).run(), 5.seconds)
      results.length mustEqual(3)

      findEntry("/path/to/file1", results) must beSome(
        FileEntry(
          LocalFile(Paths.get("/path/to/file1"), 123, Some("local-checksum")),
          Some(RemoteFile("oid-1",123,Some("remote-checksum")))
        )
      )
      findEntry("/path/to/file2", results) must beSome(
        FileEntry(
          LocalFile(Paths.get("/path/to/file2"), 234, Some("local-checksum")),
          Some(RemoteFile("oid-2",234,Some("remote-checksum")))
        )
      )
      findEntry("/path/to/file3", results) must beSome(
        FileEntry(
          LocalFile(Paths.get("/path/to/file3"), 345, Some("local-checksum")),
          Some(RemoteFile("oid-3",345,Some("remote-checksum")))
        )
      )
    }
  }
}

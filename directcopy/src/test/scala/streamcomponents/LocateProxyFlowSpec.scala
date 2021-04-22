package streamcomponents

import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import models.ToCopy
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import streamcomponents.FilenameHelpers.PathXtn

import java.nio.file.{Path, Paths}
import scala.concurrent.Await
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

class LocateProxyFlowSpec extends Specification with Mockito {
  "LocateProxyFlow.findDependent" should {
    "check for the existence of a proxy file at the same relative path as the media file" in {
      val toTest = new LocateProxyFlow(
        Paths.get("path/to/sourceMedia"),
        Paths.get("path/to/proxyMedia"),
        None,
        List(".mp4"),
        None,
        ".jpg"
      ) {
        override protected def doesPathExist(p: Path): Try[Boolean] = {
          println(s"checking $p")
          Success(
            p.toString=="path/to/proxyMedia/commission/project/media/somefile.mp4"
          )
        }

        def callFindDependent(directoryPath:Path, optionalPostfix:Option[String], targetXtn:String, fileXtn:PathXtn):Try[Option[Path]] =
          findDependent(directoryPath, optionalPostfix, targetXtn, fileXtn)
      }

      val result = toTest.callFindDependent(Paths.get("path/to/proxyMedia"),
        None,
       "mp4",
        FilenameHelpers.PathXtn("commission/project/media/somefile",Some("mxf"))
      )

      result must beSuccessfulTry
      result.get must beSome
      result.get.get.toString mustEqual "path/to/proxyMedia/commission/project/media/somefile.mp4"
    }

    "return None if the file does not exist" in {
      val toTest = new LocateProxyFlow(
        Paths.get("path/to/sourceMedia"),
        Paths.get("path/to/proxyMedia"),
        None,
        List(".mp4"),
        None,
        ".jpg"
      ) {
        override protected def doesPathExist(p: Path): Try[Boolean] = {
          Success(
            false
          )
        }

        def callFindDependent(directoryPath:Path, optionalPostfix:Option[String], targetXtn:String, fileXtn:PathXtn):Try[Option[Path]] =
          findDependent(directoryPath, optionalPostfix, targetXtn, fileXtn)
      }

      val result = toTest.callFindDependent(Paths.get("path/to/proxyMedia"),
        None,
        "mp4",
        FilenameHelpers.PathXtn("commission/project/media/somefile",Some("mxf"))
      )

      result must beSuccessfulTry
      result.get must beNone
    }

    "pass a filesystem error along" in {
      val toTest = new LocateProxyFlow(
        Paths.get("path/to/sourceMedia"),
        Paths.get("path/to/proxyMedia"),
        None,
        List(".mp4"),
        None,
        ".jpg"
      ) {
        override protected def doesPathExist(p: Path): Try[Boolean] = {
          Failure(new RuntimeException("Kaboom"))
        }

        def callFindDependent(directoryPath:Path, optionalPostfix:Option[String], targetXtn:String, fileXtn:PathXtn):Try[Option[Path]] =
          findDependent(directoryPath, optionalPostfix, targetXtn, fileXtn)
      }

      val result = toTest.callFindDependent(Paths.get("path/to/proxyMedia"),
        None,
        "mp4",
        FilenameHelpers.PathXtn("commission/project/media/somefile",Some("mxf"))
      )

      result must beFailedTry
    }
  }

  "LocateProxyFlow" should {
    "return existing proxy and thumbnail files" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val mockDoesPathExist = mock[(Path)=>Try[Boolean]]
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.mp4")) returns Success(true)
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.wav")) returns Success(false)
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg")) returns Success(true)

      val sinkFac = Sink.seq[ToCopy]
      val graph = GraphDSL.create(sinkFac) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(
          Source.single(Paths.get("/srv/volume/Media Production/Assets/wg/commission/project/media/somefile.mxf"))
        )
        val lookup = builder.add(
          new LocateProxyFlow(
            Paths.get("/srv/volume/Media Production/Assets/"),
            Paths.get("/srv/volume/Proxies"),
            None,
            List("mp4","wav"),
            None,
            "jpg"
          ) {
            override def doesPathExist(p: Path): Try[Boolean] = mockDoesPathExist(p)
          }
        )
        src ~> lookup ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 10.seconds)
      result.length mustEqual 1
      result.head.sourceFile.path.toString mustEqual "/srv/volume/Media Production/Assets/wg/commission/project/media/somefile.mxf"
      result.head.thumbnail must beSome
      result.head.thumbnail.get.path.toString mustEqual("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg")
      result.head.proxyMedia must beSome
      result.head.proxyMedia.get.path.toString mustEqual "/srv/volume/Proxies/wg/commission/project/media/somefile.mp4"

      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.mp4"))
      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.wav"))
      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg"))
    }

    "find a file that is not first in the list" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val mockDoesPathExist = mock[(Path)=>Try[Boolean]]
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.mp4")) returns Success(false)
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.wav")) returns Success(true)
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg")) returns Success(true)

      val sinkFac = Sink.seq[ToCopy]
      val graph = GraphDSL.create(sinkFac) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(
          Source.single(Paths.get("/srv/volume/Media Production/Assets/wg/commission/project/media/somefile.mxf"))
        )
        val lookup = builder.add(
          new LocateProxyFlow(
            Paths.get("/srv/volume/Media Production/Assets/"),
            Paths.get("/srv/volume/Proxies"),
            None,
            List("mp4","wav"),
            None,
            "jpg"
          ) {
            override def doesPathExist(p: Path): Try[Boolean] = mockDoesPathExist(p)
          }
        )
        src ~> lookup ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 10.seconds)
      result.length mustEqual 1
      result.head.sourceFile.path.toString mustEqual "/srv/volume/Media Production/Assets/wg/commission/project/media/somefile.mxf"
      result.head.thumbnail must beSome
      result.head.thumbnail.get.path.toString mustEqual("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg")
      result.head.proxyMedia must beSome
      result.head.proxyMedia.get.path.toString mustEqual "/srv/volume/Proxies/wg/commission/project/media/somefile.wav"

      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.mp4"))
      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.wav"))
      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg"))
    }

    "not return non-existent proxy files" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val mockDoesPathExist = mock[(Path)=>Try[Boolean]]
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.mp4")) returns Success(false)
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg")) returns Success(true)

      val sinkFac = Sink.seq[ToCopy]
      val graph = GraphDSL.create(sinkFac) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(
          Source.single(Paths.get("/srv/volume/Media Production/Assets/wg/commission/project/media/somefile.mxf"))
        )
        val lookup = builder.add(
          new LocateProxyFlow(
            Paths.get("/srv/volume/Media Production/Assets/"),
            Paths.get("/srv/volume/Proxies"),
            None,
            List("mp4"),
            None,
            "jpg"
          ) {
            override def doesPathExist(p: Path): Try[Boolean] = mockDoesPathExist(p)
          }
        )
        src ~> lookup ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 10.seconds)
      result.length mustEqual 1
      result.head.sourceFile.path.toString mustEqual "/srv/volume/Media Production/Assets/wg/commission/project/media/somefile.mxf"
      result.head.thumbnail must beSome
      result.head.thumbnail.get.path.toString mustEqual("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg")
      result.head.proxyMedia must beNone

      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.mp4"))
      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg"))
    }

    "not return non-existent thumbnail files" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val mockDoesPathExist = mock[(Path)=>Try[Boolean]]
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.mp4")) returns Success(true)
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg")) returns Success(false)

      val sinkFac = Sink.seq[ToCopy]
      val graph = GraphDSL.create(sinkFac) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(
          Source.single(Paths.get("/srv/volume/Media Production/Assets/wg/commission/project/media/somefile.mxf"))
        )
        val lookup = builder.add(
          new LocateProxyFlow(
            Paths.get("/srv/volume/Media Production/Assets/"),
            Paths.get("/srv/volume/Proxies"),
            None,
            List("mp4"),
            None,
            "jpg"
          ) {
            override def doesPathExist(p: Path): Try[Boolean] = mockDoesPathExist(p)
          }
        )
        src ~> lookup ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 10.seconds)
      result.length mustEqual 1
      result.head.sourceFile.path.toString mustEqual "/srv/volume/Media Production/Assets/wg/commission/project/media/somefile.mxf"
      result.head.thumbnail must beNone
      result.head.proxyMedia must beSome
      result.head.proxyMedia.get.path.toString mustEqual "/srv/volume/Proxies/wg/commission/project/media/somefile.mp4"

      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.mp4"))
      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg"))
    }

    "fail on a filesystem error" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val mockDoesPathExist = mock[(Path)=>Try[Boolean]]
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.mp4")) returns Success(true)
      mockDoesPathExist.apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg")) returns Failure(new RuntimeException("kaboom"))

      val sinkFac = Sink.seq[ToCopy]
      val graph = GraphDSL.create(sinkFac) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(
          Source.single(Paths.get("/srv/volume/Media Production/Assets/wg/commission/project/media/somefile.mxf"))
        )
        val lookup = builder.add(
          new LocateProxyFlow(
            Paths.get("/srv/volume/Media Production/Assets/"),
            Paths.get("/srv/volume/Proxies"),
            None,
            List("mp4"),
            None,
            "jpg"
          ) {
            override def doesPathExist(p: Path): Try[Boolean] = mockDoesPathExist(p)
          }
        )
        src ~> lookup ~> sink
        ClosedShape
      }

      val result = Try { Await.result(RunnableGraph.fromGraph(graph).run(), 10.seconds) }
      result must beFailedTry
      result.failed.get.getMessage mustEqual "Could not check thumb and/or proxy, see logs for details."
      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.mp4"))
      there was one(mockDoesPathExist).apply(Paths.get("/srv/volume/Proxies/wg/commission/project/media/somefile.jpg"))
    }
  }
}

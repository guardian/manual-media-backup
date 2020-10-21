import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.stream.{ActorMaterializer, Materializer}
import helpers.{LocalDateTimeEncoder, PlutoCommunicator, PlutoCommunicatorFuncs}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, ResponseEntity}
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
import akka.testkit.TestProbe
import models.pluto.{AssetFolderRecord, MasterRecord}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

class PlutoCommunicatorSpec extends Specification with Mockito {
  implicit val timeout:akka.util.Timeout = 10 seconds

  "AssetFolderHelper ! Lookup" should {
    "call out to pluto to get asset folder information if none is present in the cache, and store the result" in new AkkaTestkitSpecs2Support {
      import helpers.PlutoCommunicator._
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val rawServerResponse="""{"status":"ok","path":"/some/path/here","project":"VX-1234"}"""

      val mockedResponseEntity = mock[ResponseEntity]
      mockedResponseEntity.dataBytes returns Source.single(ByteString(rawServerResponse))
      val mockedHttpResponse = HttpResponse().withEntity(mockedResponseEntity)

      val mockedHttp = mock[HttpExt]
      mockedHttp.singleRequest(any,any,any,any) returns Future(mockedHttpResponse)

      val mockedSelf = TestProbe()

      val toTest = system.actorOf(Props(new PlutoCommunicator("https://pluto-base/", "secret") {
        override def callHttp: HttpExt = mockedHttp

        override val ownRef = mockedSelf.ref
      }))

      val path = new File("/some/path/here").toPath
      val result = Await.result((toTest ? Lookup(path)).mapTo[AFHMsg],10 seconds)

      result must beAnInstanceOf[FoundAssetFolder]
      result.asInstanceOf[FoundAssetFolder].result mustEqual Some(AssetFolderRecord("ok","/some/path/here","VX-1234"))

      val expectedRequest = HttpRequest(uri="https://pluto-base/gnm_asset_folder/lookup?path=%2Fsome%2Fpath%2Fhere")
      there was one(mockedHttp).singleRequest(expectedRequest)
      mockedSelf.expectMsg(StoreInCache(path, Some(AssetFolderRecord("ok","/some/path/here","VX-1234"))))
    }

    "reply from the cache if there was a result available" in new AkkaTestkitSpecs2Support {
      import helpers.PlutoCommunicator._
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val mockedHttp = mock[HttpExt]

      val mockedSelf = TestProbe()

      val toTest = system.actorOf(Props(new PlutoCommunicator("https://pluto-base/", "secret") {
        override def callHttp: HttpExt = mockedHttp

        override val ownRef = mockedSelf.ref
      }))

      val path = new File("/some/path/here").toPath

      Await.ready(toTest ? StoreInCache(path, Some(AssetFolderRecord("ok","/some/path/here","VX-456"))), 10 seconds)

      val result = Await.result((toTest ? Lookup(path)).mapTo[AFHMsg],10 seconds)

      result must beAnInstanceOf[FoundAssetFolder]
      result.asInstanceOf[FoundAssetFolder].result mustEqual Some(AssetFolderRecord("ok","/some/path/here","VX-456"))

      val expectedRequest = HttpRequest(uri="https://pluto-base/gnm_asset_folder/lookup?path=%2Fsome%2Fpath%2Fhere")
      there was no(mockedHttp).singleRequest(expectedRequest)
      mockedSelf.expectNoMessage()
    }
  }

  "PlutoCommunicatorFuncs ! callToPluto" should {
    "unmarshal the provided data and return it as a domain object" in new AkkaTestkitSpecs2Support {
      import io.circe.generic.auto._
      import LocalDateTimeEncoder._

      val returnContentString =
        """
          |[{"user": 520, "title": "Election 2019: On the campaign trail with Boris Johnson and Jeremy Corbyn", "created": "2019-12-10T13:32:47.305", "updated": "2019-12-10T13:33:01.412", "duration": null, "commission": 53046, "project": 53128, "gnm_master_standfirst": null, "gnm_master_website_headline": "Election 2019: On the campaign trail with Boris Johnson and Jeremy Corbyn", "gnm_master_generic_status": null, "gnm_master_generic_intendeduploadplatforms": null, "gnm_master_generic_publish": null, "gnm_master_generic_remove": null}]
          |""".stripMargin

      implicit val mat:Materializer = ActorMaterializer.create(system)
      val mockResponseEntity = mock[ResponseEntity]
      mockResponseEntity.dataBytes returns Source.single(ByteString(returnContentString))
      val httpMock = mock[HttpExt]
      httpMock.singleRequest(any,any,any,any) returns Future(HttpResponse(200,entity=mockResponseEntity))
      val toTest = new PlutoCommunicatorTest(httpMock)

      val req = HttpRequest()
      val result = Await.result(toTest.callCallToPluto[Seq[MasterRecord]](req), 3 seconds)
      result must beSome
      result.get.length mustEqual 1
      result.get.head.user mustEqual Some(520)
      result.get.head.title mustEqual "Election 2019: On the campaign trail with Boris Johnson and Jeremy Corbyn"
    }
  }
}

class PlutoCommunicatorTest(httpMock:HttpExt) (override implicit val system: ActorSystem, override val mat: Materializer) extends PlutoCommunicatorFuncs {
  override val plutoBaseUri: String = "http://localhost:9000"
  override val plutoSharedSecret: String = "nothing"
  override val logger = LoggerFactory.getLogger("PlutoCommnicatorTest")

  override def callHttp: HttpExt = httpMock

  def callCallToPluto[T:io.circe.Decoder](req:HttpRequest, attempt:Int=1):Future[Option[T]] = callToPluto[T](req, attempt)
}
import java.io.File

import akka.actor.Props
import akka.stream.{ActorMaterializer, Materializer}
import helpers.PlutoCommunicator
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
import models.pluto.AssetFolderRecord

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

      val toTest = system.actorOf(Props(new PlutoCommunicator("https://pluto-base/", "someuser", "somepassword") {
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

      val toTest = system.actorOf(Props(new PlutoCommunicator("https://pluto-base/", "someuser", "somepassword") {
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


}

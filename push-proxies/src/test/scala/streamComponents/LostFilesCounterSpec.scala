package streamComponents

import java.io.File
import java.time.{ZoneId, ZoneOffset, ZonedDateTime}

import akka.actor.Props
import com.gu.vidispineakka.vidispine.{VSFile, VSLazyItem, VSShape}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import akka.pattern.ask

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.io.Source

class LostFilesCounterSpec extends Specification with Mockito {
  import LostFilesCounter._
  implicit val akkaTimeout:akka.util.Timeout = 5 seconds

  "LostFilesCounter" should {
    "register lost files upon receipt of RegisterLost and dump out a CSV on receipt of the Dump message" in new AkkaTestkitSpecs2Support {
      val toTest = system.actorOf(Props(classOf[LostFilesCounter]))

      val fakeFile = mock[VSFile]
      fakeFile.vsid returns "VX-1234"
      fakeFile.path returns "path/to/somefile.ext"
      fakeFile.size returns 1234L
      fakeFile.storage returns "VX-2"
      fakeFile.timestamp returns ZonedDateTime.of(2019,1,2,3,4,5,678,ZoneId.of("UTC"))
      val fakeShape = mock[VSShape]
      fakeShape.vsid returns "VX-2345"

      val fakeItem = mock[VSLazyItem]
      fakeItem.itemId returns "VX-3456"
      fakeItem.getSingle("gnm_original_filename") returns Some("path/to/originalmedia.mxf")

     Await.result(toTest ? RegisterLost(fakeFile, fakeShape, fakeItem), 5 seconds)

      val outfile = File.createTempFile("lostfilescountertest-","")
      outfile.deleteOnExit()
      val dumpResult = Await.result(toTest ? Dump(outfile.getAbsolutePath), 5 seconds)

      val s = Source.fromFile(outfile)
      val content = s.mkString
      s.close()

      println(content)
      content.contains(
        """"VX-1234","VX-2345","VX-3456","path/to/originalmedia.mxf","VX-2","path/to/somefile.ext",1234,"2019-01-02T03:04:05.000000678Z[UTC]""""
      ) must beTrue
    }
  }
}

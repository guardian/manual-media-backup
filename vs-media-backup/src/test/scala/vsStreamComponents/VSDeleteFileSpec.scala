package vsStreamComponents

import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import models.{CopyReport, HttpError, VSBackupEntry}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import testhelpers.AkkaTestkitSpecs2Support
import vidispine.VSCommunicator

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class VSDeleteFileSpec extends Specification with Mockito {
  "VSDeleteFile" should {
    "send a delete command to the 'newly created' id on the provided storage" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)
      val vscomm = mock[VSCommunicator]
      vscomm.request(any,any,any,any,any,any)(any,any) returns Future(Right(""))

      val testData = CopyReport[VSBackupEntry]("some-filename","aaabbbcc",None,1234L,false,
        Some(false),Some(VSBackupEntry(Some("VX-2"),Some("path/to/some-filename"),Some("VX-1234"),Some("VX-1234"),None,Seq(),1,Some("VX-5678"),Some("/full/path/to/some-filename"),None,None)))

      val completionFuture = Source.single(testData).via(new VSDeleteFile(vscomm,"VX-3")).toMat(Sink.ignore)(Keep.right).run()

      Await.ready(completionFuture,30 seconds)

      there was one(vscomm).request(VSCommunicator.OperationType.DELETE,"/API/storage/VX-3/file/VX-5678",None,Map())
    }

    "retry on a 503 response" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)
      val vscomm = mock[VSCommunicator]
      vscomm.request(any,any,any,any,any,any)(any,any) returns Future(Left(HttpError("Timed out",503))) thenReturn Future(Right(""))

      val testData = CopyReport[VSBackupEntry]("some-filename","aaabbbcc",None,1234L,false,
        Some(false),Some(VSBackupEntry(Some("VX-2"),Some("path/to/some-filename"),Some("VX-1234"),Some("VX-1234"),None,Seq(),1,Some("VX-5678"),Some("/full/path/to/some-filename"),None,None)))

      val completionFuture = Source.single(testData).via(new VSDeleteFile(vscomm,"VX-3")).toMat(Sink.ignore)(Keep.right).run()

      Await.ready(completionFuture,30 seconds)

      there were one(vscomm).request(VSCommunicator.OperationType.DELETE,"/API/storage/VX-3/file/VX-5678",None,Map())
    }

  }
}

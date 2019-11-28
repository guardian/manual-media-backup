package streamcomponents

import java.io.File

import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import com.om.mxs.client.japi.{MxsObject, UserInfo, Vault}
import models.{BackupEntry, ObjectMatrixEntry}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import scala.util.{Failure, Success, Try}
import akka.stream.scaladsl.GraphDSL.Implicits._

import scala.concurrent.duration._
import scala.concurrent.Await

class CheckOMFileSpec extends Specification with Mockito {
  "CheckOMFile" should {
    "push to YES if the file search returns a result" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val fakeUserInfo = mock[UserInfo]
      val fakeVault = mock[Vault]
      val gotResult = mock[ObjectMatrixEntry]
      gotResult.oid returns "123455667"
      gotResult.attributes returns None
      gotResult.fileAttribues returns None
      gotResult.getMxsObject(any) returns mock[MxsObject]
      gotResult.getMetadataSync(any) returns gotResult

      val toTest = new CheckOMFile(fakeUserInfo) {
        override def callFindByFilename(vault: Vault, path: String): Try[Seq[ObjectMatrixEntry]] = Success(Seq(gotResult))

        override def callOpenVault: Some[Vault] = Some(fakeVault)
      }

      val incomingEntry = BackupEntry(new File("/path/to/something").toPath, None)

      val sinkFac = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sinkFac) {implicit builder=> sink=>
        val src = Source.single(incomingEntry)
        val checkOMFile = builder.add(toTest)
        val ignoreSink = Sink.ignore

        src ~> checkOMFile
        checkOMFile.out(0) ~> sink          //"yes" branch
        checkOMFile.out(1) ~> ignoreSink    //"no" branch
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      result.length mustEqual 1
      result.head.maybeObjectMatrixEntry must beSome(gotResult)
      result.head.originalPath mustEqual incomingEntry.originalPath
      there was one(fakeVault).dispose
    }

    "push to NO if the file search returns no result" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val fakeUserInfo = mock[UserInfo]
      val fakeVault = mock[Vault]

      val toTest = new CheckOMFile(fakeUserInfo) {
        override def callFindByFilename(vault: Vault, path: String): Try[Seq[ObjectMatrixEntry]] = Success(Seq())

        override def callOpenVault: Some[Vault] = Some(fakeVault)
      }

      val incomingEntry = BackupEntry(new File("/path/to/something").toPath, None)

      val sinkFac = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sinkFac) {implicit builder=> sink=>
        val src = Source.single(incomingEntry)
        val checkOMFile = builder.add(toTest)
        val ignoreSink = Sink.ignore

        src ~> checkOMFile
        checkOMFile.out(0) ~> ignoreSink          //"yes" branch
        checkOMFile.out(1) ~> sink    //"no" branch
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      result.length mustEqual 1
      result.head.maybeObjectMatrixEntry must beNone
      result.head.originalPath mustEqual incomingEntry.originalPath
      there was one(fakeVault).dispose
    }

    "fail if the lookup errors" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val fakeUserInfo = mock[UserInfo]
      val fakeVault = mock[Vault]

      val toTest = new CheckOMFile(fakeUserInfo) {
        override def callFindByFilename(vault: Vault, path: String): Try[Seq[ObjectMatrixEntry]] = Failure(new RuntimeException("My hovercraft is full of eels"))

        override def callOpenVault: Some[Vault] = Some(fakeVault)
      }

      val incomingEntry = BackupEntry(new File("/path/to/something").toPath, None)

      val sinkFac = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sinkFac) {implicit builder=> sink=>
        val src = Source.single(incomingEntry)
        val checkOMFile = builder.add(toTest)
        val ignoreSink = Sink.ignore

        src ~> checkOMFile
        checkOMFile.out(0) ~> ignoreSink          //"yes" branch
        checkOMFile.out(1) ~> sink    //"no" branch
        ClosedShape
      }

      def theTest = {
        Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      }
      theTest must throwA[RuntimeException]("My hovercraft is full of eels")
      there was one(fakeVault).dispose
    }
  }
}

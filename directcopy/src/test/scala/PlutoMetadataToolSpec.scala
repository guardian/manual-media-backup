import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import helpers.PlutoCommunicator._
import models.pluto.{AssetFolderRecord, CommissionRecord, ProjectRecord, WorkingGroupRecord}
import models.{FileInstance, MxsMetadata, PathTransformSet, ToCopy}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.nio.file.{Path, Paths}
import java.time.{LocalDateTime, ZonedDateTime}
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try

class PlutoMetadataToolSpec extends Specification with Mockito {
  "PlutoMetadataTool.addPlutoMetadata" should {
    "return ToCopy with updated metadata" in new AkkaTestkitSpecs2Support {
      val mockExistingMetadata = MxsMetadata.empty().withString("fake-key","fake-value")
      val mockRequest = ToCopy(
        FileInstance(Paths.get("/path/to/media/file.mxf")),
        Some(FileInstance(Paths.get("/path/to/proxy/media/file.mp4"))),
        Some(FileInstance(Paths.get("/path/to/proxy/media/file.jpg"))),
        Some(mockExistingMetadata)
      )

      val mockCommunicator = TestProbe()

      //defines the interaction we want our test actor to undertake
      mockCommunicator.setAutoPilot(new TestActor.AutoPilot {
        override def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case Lookup(forPath:Path)=>
              sender ! FoundAssetFolder(Some(AssetFolderRecord("ok","working-group/commission/project",1234)))
              TestActor.KeepRunning
            case LookupProject(projectId:Int)=>
              sender ! FoundProject(Some(ProjectRecord(1234,22,None,"Test-Project",ZonedDateTime.now(), ZonedDateTime.now(), "test-user", Some(33), Some(44),None, None,None,"In Production","UK")))
              TestActor.KeepRunning
            case LookupCommission(commissionId:Option[Int])=>
              sender ! FoundCommission(Some(CommissionRecord(44, ZonedDateTime.now(), ZonedDateTime.now(), "Test Commission", "In Production", 33, LocalDateTime.now(),"test-user","UK")))
              TestActor.KeepRunning
            case LookupWorkingGroup(wgId:Int)=>
              sender ! FoundWorkingGroup(Some(WorkingGroupRecord(33,"Test working group",Some(false),"Fred the Shred")))
              TestActor.KeepRunning
            case _=>
              println(s"ERROR mock actor received unexpected message of type ${msg.getClass.getCanonicalName}")
              TestActor.NoAutoPilot
          }
        }
      })

      val toTest = new PlutoMetadataTool(mockCommunicator.ref, PathTransformSet.empty)
      val resultFut = toTest.addPlutoMetadata(mockRequest)


      val result = Await.result(resultFut, 10.seconds)
      result.commonMetadata must beSome
      val meta = result.commonMetadata.get
      meta.stringValues.get("fake-key") must beSome("fake-value")
      meta.stringValues.get("GNM_PROJECT_ID") must beSome("1234")
      meta.stringValues.get("GNM_COMMISSION_ID") must beSome("44")
      meta.stringValues.get("GNM_WORKING_GROUP_NAME") must beSome("Test working group")
      meta.stringValues.get("GNM_PROJECT_NAME") must beSome("Test-Project")
      meta.stringValues.get("GNM_COMMISSION_NAME") must beSome("Test Commission")
    }

    "return ToCopy with unchanged metadata if no asset folder is found" in new AkkaTestkitSpecs2Support {
      val mockExistingMetadata = MxsMetadata.empty().withString("fake-key","fake-value")
      val mockRequest = ToCopy(
        FileInstance(Paths.get("/path/to/media/file.mxf")),
        Some(FileInstance(Paths.get("/path/to/proxy/media/file.mp4"))),
        Some(FileInstance(Paths.get("/path/to/proxy/media/file.jpg"))),
        Some(mockExistingMetadata)
      )

      val mockCommunicator = TestProbe()

      //defines the interaction we want our test actor to undertake
      mockCommunicator.setAutoPilot(new TestActor.AutoPilot {
        override def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case Lookup(forPath:Path)=>
              sender ! FoundAssetFolder(None)
              TestActor.KeepRunning
            case _=>
              println(s"ERROR mock actor received unexpected message of type ${msg.getClass.getCanonicalName}")
              TestActor.NoAutoPilot
          }
        }
      })

      val toTest = new PlutoMetadataTool(mockCommunicator.ref, PathTransformSet.empty)
      val resultFut = toTest.addPlutoMetadata(mockRequest)

      val result = Await.result(resultFut, 10.seconds)
      result.commonMetadata.get mustEqual mockExistingMetadata
    }

    "return a failure if lookup fails" in new AkkaTestkitSpecs2Support {
      val mockExistingMetadata = MxsMetadata.empty().withString("fake-key","fake-value")
      val mockRequest = ToCopy(
        FileInstance(Paths.get("/path/to/media/file.mxf")),
        Some(FileInstance(Paths.get("/path/to/proxy/media/file.mp4"))),
        Some(FileInstance(Paths.get("/path/to/proxy/media/file.jpg"))),
        Some(mockExistingMetadata)
      )

      val mockCommunicator = TestProbe()

      //defines the interaction we want our test actor to undertake
      mockCommunicator.setAutoPilot(new TestActor.AutoPilot {
        override def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case Lookup(forPath:Path)=>
              sender ! LookupFailed
              TestActor.KeepRunning
            case _=>
              println(s"ERROR mock actor received unexpected message of type ${msg.getClass.getCanonicalName}")
              TestActor.NoAutoPilot
          }
        }
      })

      val toTest = new PlutoMetadataTool(mockCommunicator.ref, PathTransformSet.empty)
      val resultFut = toTest.addPlutoMetadata(mockRequest)

      val result = Try { Await.result(resultFut, 10.seconds) }
      result must beFailedTry
    }
  }
}

package streamcomponents

import java.io.File
import java.nio.file.Path
import java.time.{ZoneId, ZonedDateTime}

import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import models.{BackupEntry, FileAttributes, MxsMetadata, ObjectMatrixEntry}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import akka.stream.scaladsl.GraphDSL.Implicits._

import scala.concurrent.Await
import scala.concurrent.duration._

class NeedsBackupSwitchSpec extends Specification with Mockito {
  "NeedsBackupSwitch" should {
    "push to YES if the mtime of the File is after the mtime of the ObjectMatrixEntry" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val mockedSourceFile = mock[File]
      mockedSourceFile.lastModified() returns ZonedDateTime.of(2019,3,3,4,5,6,0,ZoneId.systemDefault()).toInstant.getEpochSecond*1000
      val mockedSourcePath = mock[Path]
      mockedSourcePath.toFile returns mockedSourceFile

      val backupTime = ZonedDateTime.of(2019,2,3,4,5,6,0,ZoneId.systemDefault())

      val mockedOMEntry = mock[ObjectMatrixEntry]
      mockedOMEntry.fileAttribues returns Some(FileAttributes("","filename","parent",false,false,true,false,backupTime,backupTime,backupTime,123456L))
      mockedOMEntry.attributes returns Some(MxsMetadata(Map(),Map("GNM_BEING_WRITTEN"->false),Map(),Map(),Map()))
      val incomingEntry = BackupEntry(mockedSourcePath, Some(mockedOMEntry))

      val sink = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sink) { implicit builder=> sink=>
        val src = Source.single(incomingEntry)
        val switch = builder.add(new NeedsBackupSwitch)
        val ingoreSink = Sink.ignore

        src ~> switch
        switch.out(0) ~> sink
        switch.out(1) ~> ingoreSink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      result.length mustEqual 1
      result.head mustEqual incomingEntry
    }

    "push to YES if the mtime of the File is after the mtime of the ObjectMatrixEntry and there are no attributes" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val mockedSourceFile = mock[File]
      mockedSourceFile.lastModified() returns ZonedDateTime.of(2019,3,3,4,5,6,0,ZoneId.systemDefault()).toInstant.getEpochSecond*1000
      val mockedSourcePath = mock[Path]
      mockedSourcePath.toFile returns mockedSourceFile

      val backupTime = ZonedDateTime.of(2019,2,3,4,5,6,0,ZoneId.systemDefault())

      val mockedOMEntry = mock[ObjectMatrixEntry]
      mockedOMEntry.fileAttribues returns Some(FileAttributes("","filename","parent",false,false,true,false,backupTime,backupTime,backupTime,123456L))
      mockedOMEntry.attributes returns None
      val incomingEntry = BackupEntry(mockedSourcePath, Some(mockedOMEntry))

      val sink = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sink) { implicit builder=> sink=>
        val src = Source.single(incomingEntry)
        val switch = builder.add(new NeedsBackupSwitch)
        val ingoreSink = Sink.ignore

        src ~> switch
        switch.out(0) ~> sink
        switch.out(1) ~> ingoreSink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      result.length mustEqual 1
      result.head mustEqual incomingEntry
    }

    "push to NO if the mtime of the File is before the mtime of the ObjectMatrixEntry" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val mockedSourceFile = mock[File]
      mockedSourceFile.lastModified() returns ZonedDateTime.of(2019,2,3,4,5,6,0,ZoneId.systemDefault()).toInstant.getEpochSecond*1000
      val mockedSourcePath = mock[Path]
      mockedSourcePath.toFile returns mockedSourceFile

      val backupTime = ZonedDateTime.of(2019,3,3,4,5,6,0,ZoneId.systemDefault())

      val mockedOMEntry = mock[ObjectMatrixEntry]
      mockedOMEntry.attributes returns Some(MxsMetadata(Map(),Map("GNM_BEING_WRITTEN"->false),Map(),Map(),Map()))
      mockedOMEntry.fileAttribues returns Some(FileAttributes("","filename","parent",false,false,true,false,backupTime,backupTime,backupTime,123456L))
      val incomingEntry = BackupEntry(mockedSourcePath, Some(mockedOMEntry))

      val sink = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sink) { implicit builder=> sink=>
        val src = Source.single(incomingEntry)
        val switch = builder.add(new NeedsBackupSwitch)
        val ignoreSink = Sink.ignore

        src ~> switch
        switch.out(0) ~> ignoreSink
        switch.out(1) ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      result.length mustEqual 1
      result.head mustEqual incomingEntry
    }

    "push to NO if the mtime of the File is equal to the mtime of the ObjectMatrixEntry" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val mockedSourceFile = mock[File]
      mockedSourceFile.lastModified() returns ZonedDateTime.of(2019,2,3,4,5,6,0,ZoneId.systemDefault()).toInstant.getEpochSecond*1000
      val mockedSourcePath = mock[Path]
      mockedSourcePath.toFile returns mockedSourceFile

      val backupTime = ZonedDateTime.of(2019,2,3,4,5,6,0,ZoneId.systemDefault())

      val mockedOMEntry = mock[ObjectMatrixEntry]
      mockedOMEntry.attributes returns Some(MxsMetadata(Map(),Map("GNM_BEING_WRITTEN"->false),Map(),Map(),Map()))
      mockedOMEntry.fileAttribues returns Some(FileAttributes("","filename","parent",false,false,true,false,backupTime,backupTime,backupTime,123456L))
      val incomingEntry = BackupEntry(mockedSourcePath, Some(mockedOMEntry))

      val sink = Sink.seq[BackupEntry]
      val graph = GraphDSL.create(sink) { implicit builder=> sink=>
        val src = Source.single(incomingEntry)
        val switch = builder.add(new NeedsBackupSwitch)
        val ignoreSink = Sink.ignore

        src ~> switch
        switch.out(0) ~> ignoreSink
        switch.out(1) ~> sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 3 seconds)
      result.length mustEqual 1
      result.head mustEqual incomingEntry
    }

  }
}

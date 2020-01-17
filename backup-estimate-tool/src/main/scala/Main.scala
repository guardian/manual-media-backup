import java.io.File
import java.nio.file.Path
import java.time.{Instant, ZoneId, ZonedDateTime}

import akka.actor.{ActorSystem, Props}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, Keep, RunnableGraph, Sink}
import com.om.mxs.client.japi.{Constants, SearchTerm, UserInfo}
import models.{BackupEstimateEntry, BackupEstimateGroup}
import org.slf4j.LoggerFactory
import streamcomponents.{BackupEstimateGroupSink, FileListSource, OMFastSearchSource}

import scala.concurrent.ExecutionContext.Implicits.global

object Main {
  implicit val system:ActorSystem = ActorSystem("backup-estimate-tool")
  implicit val mat:Materializer = ActorMaterializer.create(system)

  val logger = LoggerFactory.getLogger(getClass)

  val estimateActor = system.actorOf(Props(classOf[BackupEstimateGroup]))

  /**
    * build and run a stream to fetch files info from source path and put it into the BackupsEstimateGroup actor
    * @param startAt path to start from
    * @return
    */
  def getFileSystemContent(startAt:Path) = {
    FileListSource(startAt)
      .map(_.toFile).async
      .filter(_.exists())
      .map(file=>BackupEstimateEntry(file.getAbsolutePath,
        file.length(),
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(file.lastModified()), ZoneId.systemDefault())
      ))
      .map(entry=>estimateActor ! BackupEstimateGroup.AddToGroup(entry))
      .toMat(Sink.ignore)(Keep.right)
      .run()
  }

  /**
    * build and run a stream to fetch corresponding information from the backup appliance
    * @param userInfo UserInfo object describing the appliance to target
    * @return
    */
  def getObjectMatrixContent(userInfo:UserInfo) = {
    val searchTerm = SearchTerm.createSimpleTerm(Constants.CONTENT, "*")
    val interestingFields = Array("MXFS_FILENAME","DPSP_SIZE","MXFS_MODIFICATION_TIME")
    val sinkFact = Sink.seq[BackupEstimateEntry]

    val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new OMFastSearchSource(userInfo,Array(searchTerm), interestingFields, atOnce=50))
      src.out.async.map(entry=>{
        try {
          val fileSize = entry.attributes.get.stringValues("DPSP_SIZE").toLong
          val modTimeMillis = entry.attributes.get.stringValues("MXFS_MODIFICATION_TIME").toLong
          val modTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(modTimeMillis), ZoneId.systemDefault())
          Some(BackupEstimateEntry(entry.maybeGetFilename().get, fileSize, modTime))
        } catch {
          case err:Throwable=>
            logger.warn(s"Could not inspect objectmatrix file ${entry.oid}: ", err)
            None
        }
      }).filter(_.isDefined).map(_.get) ~> sink

      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }
}

import java.io.File
import java.nio.charset.Charset
import java.nio.file.Path

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Framing, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.util.ByteString
import archivehunter.ArchiveHunterLookup
import com.om.mxs.client.japi.UserInfo
import helpers.{AlpakkaS3Uploader, VSHelpers}
import models.{ArchiveTargetStatus, PotentialArchiveTarget, S3Target, VSConfig}
import org.slf4j.LoggerFactory
import streamcomponents.ArchiveHunterFileSizeSwitch
import vidispine.VSCommunicator
import vsStreamComponents.VSDeleteShapeAndOrFile
import com.softwaremill.sttp._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val actorSystem:ActorSystem = ActorSystem.create("unclog-vidispine")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  val ahBaseUri = sys.env("ARCHIVE_HUNTER_URL")
  val ahSecret = sys.env("ARCHIVE_HUNTER_SECRET")

  val targetBucket = sys.env("TARGET_BUCKET")

  lazy val vsConfig = VSConfig(
    uri"${sys.env("VIDISPINE_BASE_URL")}",
    sys.env("VIDISPINE_USER"),
    sys.env("VIDISPINE_PASSWORD")
  )

  lazy val chunkSize = sys.env.getOrElse("CHUNK_SIZE","1024").toInt //chunk size in kByte/s

  lazy implicit val vsCommunicator = new VSCommunicator(vsConfig.vsUri, vsConfig.plutoUser, vsConfig.plutoPass)

  val vaultFile = sys.env("VAULT_FILE")
  val readingFrom = sys.env("NDJSON_LIST")

  def terminate(exitCode:Int) = actorSystem.terminate().andThen({
    case _=>System.exit(exitCode)
  })

  def buildStream(sourceFile:Path, userInfo:UserInfo) = {
    val sinkFact = Sink.seq[PotentialArchiveTarget]
    val fssFact = new ArchiveHunterFileSizeSwitch

    GraphDSL.create(sinkFact) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val ahLookup = builder.add(new ArchiveHunterLookup(ahBaseUri, ahSecret))
      val archiveSizeCheck = builder.add(fssFact)
      val uploader = new AlpakkaS3Uploader(userInfo)
      val postUploadSizeCheck = builder.add(fssFact)

      val canDeleteMerge = builder.add(Merge[PotentialArchiveTarget](2))
      val deleter = builder.add(new VSDeleteShapeAndOrFile())

      val outputMerge = builder.add(Merge[PotentialArchiveTarget](3))
      val src = builder.add(Source.fromGraph(FileIO
        .fromPath(sourceFile)
        .via(Framing.delimiter(ByteString("\n"),10240000,allowTruncation=false))
        .map(recordBytes=>PotentialArchiveTarget.fromMediaCensusJson(recordBytes.decodeString("UTF-8")))
        .map({
          case Success(target)=>target
          case Failure(err)=>
            logger.error(s"Could not decode incoming target: ", err)
            throw err
        })
      ))

      src.out ~> ahLookup
      ahLookup.out(0) ~> archiveSizeCheck                                                                 //"YES" branch - item already exists, check file sizes
      ahLookup.out(1).mapAsyncUnordered(4)(entry=>{                                            //"NO" branch  - item does not exist in archive, upload it
        val target = S3Target(targetBucket, entry.mxsFilename)
        uploader.performS3Upload(entry.oid,entry.contentType, target).map(r=>{
          logger.info(s"Uploaded ${entry.mxsFilename} (${entry.oid}) to $target")
          entry.copy(status = ArchiveTargetStatus.SUCCESS, archivedSize = Some(r.getContentLength))
        })
      }) ~> postUploadSizeCheck

      postUploadSizeCheck.out(0) ~> canDeleteMerge                                                        //"YES" branch postupload - uploaded item file size matches so we are clear to delete
      postUploadSizeCheck.out(1).map(_.copy(status=ArchiveTargetStatus.UPLOAD_FAILED)) ~> outputMerge     //"NO" branch postupload  - uploaded item file size does not match so we should not delete

      archiveSizeCheck.out(1).map(_.copy(status = ArchiveTargetStatus.TARGET_CONFLICT)) ~> outputMerge    //"NO"  branch alreadyexists - file sizes do not match, we have a conflict
      archiveSizeCheck.out(0) ~> canDeleteMerge                                                           //"YES" branch alreadyexists - item already exists in archive with matching size, can be deleted

      canDeleteMerge.out.mapAsync(4)(elem=>{
        elem.vsItemAttachment match {
          case Some(vsItemId)=>
            VSHelpers.setArchivalMetadataFields(vsItemId, elem.uploadedTarget.get).map({
              case Left(err)=>
                logger.error(s"Could not update VS archival fields on $vsItemId: $err")
                throw new RuntimeException("could not update VS item, see logs for error")
              case Right(_)=>
                logger.info(s"Updated VS archival fields on $vsItemId for ${elem.vsFileId}")
                elem
            })
        }
      }) ~> deleter
      deleter.out.map(_.copy(status = ArchiveTargetStatus.SUCCESS)) ~> outputMerge

      outputMerge ~> sink
      ClosedShape
    }
  }

  /**
   * returns the item count and total size for all items in the given state
   * @param results complete sequence of results
   * @param forStatus status value to filter
   * @return a tuple of (count of items, sum of byteSize)
   */
  def totalUpStatus(results:Seq[PotentialArchiveTarget], forStatus:ArchiveTargetStatus.Value) = {
    val matches = results.filter(_.status==forStatus)
    val totalSize = matches.foldLeft(0L)((acc,elem)=>acc+elem.byteSize.getOrElse(0))
    val totalCount = matches.length
    (totalCount, totalSize)
  }

  def gb(forNum:Long):Long = math.ceil(forNum/math.pow(1024,3)).toLong

  def main(args: Array[String]): Unit = {
    UserInfoBuilder.fromFile(vaultFile) match {
      case Failure(err)=>
        logger.error(s"Could not get vault information from $vaultFile: ", err)
        terminate(1)
      case Success(userInfo)=>
        val readingFromFile = new File(readingFrom)
        val graph = buildStream(readingFromFile.toPath, userInfo)
        RunnableGraph.fromGraph(graph).run().onComplete({
          case Failure(err)=>
            logger.error("Main stream crashed: ",err)
            terminate(2)
          case Success(results)=>
            val (successCount, successSize) = totalUpStatus(results, ArchiveTargetStatus.SUCCESS)
            val (delFailureCount, delFailureSize) = totalUpStatus(results, ArchiveTargetStatus.DELETE_FAILED)
            val (upFailureCount, upFailureSize) = totalUpStatus(results, ArchiveTargetStatus.UPLOAD_FAILED)
            val (conflictCount, conflictSize) = totalUpStatus(results, ArchiveTargetStatus.TARGET_CONFLICT)
            val (stillInProgCount, stillInProgSize) = totalUpStatus(results, ArchiveTargetStatus.IN_PROGRESS) //should always be zero!

            logger.info("Run completed")
            logger.info(s"Successful items: $successCount totalling ${gb(successSize)} Gib")
            logger.info(s"Deletion failures: $delFailureCount totalling ${gb(delFailureSize)} Gib")
            logger.info(s"Upload failures: $upFailureCount totalling ${gb(upFailureSize)} Gib")
            logger.info(s"Upload conflicts: $conflictCount totalling ${gb(conflictSize)} Gib")
            logger.info(s"Still in progress (?): $stillInProgCount totalling ${gb(stillInProgSize)}")
            terminate(0)
        })
    }
  }
}

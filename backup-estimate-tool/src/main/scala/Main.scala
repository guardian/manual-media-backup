import java.io.File
import java.nio.file.Path
import java.time.{Instant, ZoneId, ZonedDateTime}

import akka.actor.{ActorSystem, Props}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, Keep, RunnableGraph, Sink, Source}
import com.om.mxs.client.japi.{Constants, SearchTerm, UserInfo}
import models.BackupEstimateGroup.{BEMsg, FoundEntry, NotFoundEntry}
import models.{BackupEstimateEntry, BackupEstimateGroup, EstimateCounter, FinalEstimate}
import org.slf4j.LoggerFactory
import streamcomponents.{BackupEstimateGroupSink, FileListSource, OMFastSearchSource, UTF8PathCharset}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.duration._

object Main {
  implicit val system:ActorSystem = ActorSystem("backup-estimate-tool")
  implicit val mat:Materializer = ActorMaterializer.create(system)

  val logger = LoggerFactory.getLogger(getClass)

  val estimateActor = system.actorOf(Props(classOf[BackupEstimateGroup]))


  def buildOptionParser = new scopt.OptionParser[Options]("backup-estimate-tool") {
    head("backup-estimate-tool","1.x")

    opt[String]("vault-file").action((x,c)=>c.copy(vaultFile = x)).text(".vault file from MatrixStoreAdmin describing the vault to target")
    opt[String]("local-path").action((x,c)=>c.copy(localPath = x)).text("local path to scan")
  }

  /**
    * build and run a stream to fetch files info from source path and put it into the BackupsEstimateGroup actor
    * @param startAt path to start from
    * @return
    */
  def getFileSystemContent(startAt:Path) = {
    FileListSource(startAt)
      .via(UTF8PathCharset())
      .map(_.toFile).async
      .filter(_.exists())
      .map(file=>BackupEstimateEntry(file.getAbsolutePath,
        file.length(),
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(file.lastModified()), ZoneId.systemDefault())
      ))
      //.map(entry=>estimateActor ! BackupEstimateGroup.AddToGroup(entry))
      .toMat(Sink.seq)(Keep.right)
      .run()
  }

  /**
    * build and run a stream to fetch corresponding information from the backup appliance
    * results are stored in the BackupEstimateGroup actor
    * @param userInfo UserInfo object describing the appliance to target
    * @return a Future that resolves when the stream has completed
    */
  def getObjectMatrixContent(userInfo:UserInfo) = {
    val searchTerm = SearchTerm.createSimpleTerm(Constants.CONTENT, "*")
    val interestingFields = Array("MXFS_FILENAME","DPSP_SIZE","MXFS_MODIFICATION_TIME")
    val sinkFact = Sink.ignore

    val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      implicit val timeout:akka.util.Timeout = 2.seconds
      val src = builder.add(new OMFastSearchSource(userInfo,Array(searchTerm), interestingFields, atOnce=50))
      src.out.async
        .map(entry=>{
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
        })
        .filter(_.isDefined)
        .map(ent=>BackupEstimateGroup.AddToGroup(ent.get))
        .ask(estimateActor) ~> sink

      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }

  def performCrossMatch(checkList:Seq[BackupEstimateEntry]) = {
    import akka.pattern.ask
    implicit val timeout:akka.util.Timeout = 5 seconds
    val graph = GraphDSL.create() { implicit builder=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = Source.fromIterator(()=>checkList.toIterator)
      ClosedShape
    }

    Source.fromIterator(()=>checkList.toIterator)
      .mapAsyncUnordered(10)(entry=>(estimateActor ? BackupEstimateGroup.FindEntryFor(entry.filePath)).mapTo[BEMsg].map({
        case FoundEntry(entries)=>
          if(entries.length>1){
            logger.warn(s"Found ${entries.length} entries for ${entry.filePath}")
          }
          val potentialBackups = entries.filter(_.mTime==entry.mTime).filter(_.size==entry.size)
          if(potentialBackups.nonEmpty){
            logger.debug(s"Entry $entry matched ${potentialBackups.head} and ${potentialBackups.tail.length} others")
            EstimateCounter(0L,entry.size)
          } else {
            logger.debug(s"Entry $entry matched nothing")
            EstimateCounter(entry.size,0L)
          }
        case NotFoundEntry=>
          EstimateCounter(entry.size,0L)
      }))
      .toMat(Sink.fold[FinalEstimate, EstimateCounter](FinalEstimate.empty)((acc,elem)=>{
        if(elem.needsBackupSize>0){
          acc.copy(needsBackupCount = acc.needsBackupCount+1,needsBackupSize = acc.needsBackupSize+elem.needsBackupSize)
        } else if(elem.noBackupSize>0){
          acc.copy(noBackupCount = acc.noBackupCount+1,noBackupSize = acc.noBackupSize+elem.noBackupSize)
        }else {
          logger.error(s"Got element with zero for needs backup and no backup, this should not happen")
          throw new RuntimeException("Unexpected element in counter, see logs")
        }
      }))(Keep.right)
      .run()
  }

  def main(args: Array[String]): Unit = {
    buildOptionParser.parse(args,Options()) match {
      case None=>
        logger.error("You must specify commandline options. Try --help for starters.")
      case Some(opts)=>
        UserInfoBuilder.fromFile(opts.vaultFile) match {
          case Failure(err)=>
            logger.error(s"Could not read vault information from ${opts.vaultFile}: ", err)
          case Success(userInfo)=>
            logger.info(s"Starting file scan from ${opts.localPath}....")
            val fileScanFuture = getFileSystemContent(new File(opts.localPath).toPath)

            logger.info(s"Starting remote scan from ${opts.vaultFile}...")
            val vaultScanFuture = getObjectMatrixContent(userInfo)

            Future.sequence(Seq(fileScanFuture, vaultScanFuture))
            .flatMap(results=>{
              //remote content is held in the actor at estimateActor. This is to quickly access it via maps.
              val localContentSeq = results.head.asInstanceOf[Seq[BackupEstimateEntry]]
              logger.info(s"Completed remote and local file scans. Got ${localContentSeq.length} local entries")

              performCrossMatch(localContentSeq)
            })
            .onComplete({
              case Success(finalEstimate)=>
              case Failure(err)=>
            })
        }
    }
  }
}

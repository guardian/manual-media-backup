import java.io.File
import java.nio.file.Path
import java.time.{Instant, ZoneId, ZonedDateTime}

import akka.actor.{ActorSystem, Props}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, Keep, RunnableGraph, Sink, Source}
import com.om.mxs.client.japi.{Constants, SearchTerm, UserInfo}
import models.BackupEstimateGroup.{BEMsg, FoundEntry, NotFoundEntry, SizeReturn}
import models.{BackupEstimateEntry, BackupEstimateGroup, EstimateCounter, FinalEstimate}
import org.slf4j.LoggerFactory
import streamcomponents.{BackupEstimateGroupSink, ExcludeListSwitch, FileListSource, FilterOutDirectories, FilterOutMacStuff, OMFastSearchSource, UTF8PathCharset}

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
    opt[String]("report-path").action((x,c)=>c.copy(reportOutputFile = Some(x))).text("Writable path to output a backup report to")
    opt[String]("exclude-paths-file").action((x,c)=>c.copy(excludePathsFile=Some(x))).text("A Json file that gives an array of filepaths to exclude as regexes")
    opt[Int]("matching-parallelism").action((x,c)=>c.copy(matchParallel = x)).text("How many parallel processes to run when cross-referencing matches")
  }

  /**
    * build and run a stream to fetch files info from source path and put it into the BackupsEstimateGroup actor
    * @param startAt path to start from
    * @return
    */
  def getFileSystemContent(startAt:Path, excludeListFile:Option[String]) = {
    FileListSource(startAt)
      .via(new FilterOutDirectories)
      .via(UTF8PathCharset())
      .via(new FilterOutMacStuff)
      .via(new ExcludeListSwitch(excludeListFile))
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
    //we don't seem to get whatever field is first in the list :(
    val searchTerm = SearchTerm.createSimpleTerm(Constants.CONTENT, "*\nkeywords: __mxs__id,__mxs__location,MXFS_FILENAME,DPSP_SIZE,MXFS_MODIFICATION_TIME\n")
    val interestingFields = Array("MXFS_FILENAME","DPSP_SIZE","MXFS_MODIFICATION_TIME")
    val sinkFact = Sink.ignore

    val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      implicit val timeout:akka.util.Timeout = 2.seconds
      val src = builder.add(new OMFastSearchSource(userInfo,Array(searchTerm), interestingFields, contentSearchBareTerm=true,atOnce=50).async)
      src.out
        .map(entry=>{
          try {
            val fileSize = entry.attributes.get.stringValues("DPSP_SIZE").toLong
            val modTimeMillis = entry.attributes.get.stringValues("MXFS_MODIFICATION_TIME").toLong
            val modTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(modTimeMillis), ZoneId.systemDefault())
            Some(BackupEstimateEntry(entry.maybeGetFilename().get, fileSize, modTime))
          } catch {
            case err:Throwable=>
              logger.warn(s"Could not inspect objectmatrix file $entry: ", err)
              None
          }
        })
        .filter(_.isDefined)
        .map(ent=>BackupEstimateGroup.AddToGroup(ent.get))
        .ask[Unit](estimateActor) ~> sink

      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }

  def performCrossMatch(checkList:Seq[BackupEstimateEntry]) = {
    import akka.pattern.ask
    implicit val timeout:akka.util.Timeout = 5 seconds

    (estimateActor ? BackupEstimateGroup.DumpContent).flatMap({
      case contentImg:Map[String,Seq[BackupEstimateEntry]] => //the framework drops the surrounding "success" message
        logger.info(s"Starting crossmatch process...")
        Source.fromIterator(() => checkList.toIterator)
          .mapAsyncUnordered(10)(entry=>Future {
            contentImg.get(entry.filePath) match {
              case None=>
                logger.info(s"Entry $entry had no matches")
                EstimateCounter(Some(entry.size),None)
              case Some(potentialBackups)=>
                val matches = potentialBackups.filter(_.size==entry.size)
                if(matches.nonEmpty){
                  logger.debug(s"Entry $entry matched ${matches.head} and ${matches.tail.length} others")
                  EstimateCounter(None, Some(entry.size))
                } else {
                  logger.debug(s"Entry $entry matched nothing out of $potentialBackups")
                  EstimateCounter(Some(entry.size), None)
                }
            }
          })
          .toMat(Sink.fold[FinalEstimate, EstimateCounter](FinalEstimate.empty)((acc, elem) => {
            if (elem.needsBackupSize.isDefined) {
              acc.copy(needsBackupCount = acc.needsBackupCount + 1, needsBackupSize = acc.needsBackupSize + elem.needsBackupSize.get)
            } else if (elem.noBackupSize.isDefined) {
              acc.copy(noBackupCount = acc.noBackupCount + 1, noBackupSize = acc.noBackupSize + elem.noBackupSize.get)
            } else {
              logger.error(s"Got element with nothing defined for needs backup *and* no backup, this should not happen")
              throw new RuntimeException("Unexpected element in counter, see logs")
            }
          }))(Keep.right)
          .run()
      case wrongMsg @ _=>
        logger.error(s"Got an unexpected message from BackupEstimateGroup: $wrongMsg")
        Future.failed(new RuntimeException("Got an unexpected message from BackupEstimateGroup"))
    })
  }

  def main(args: Array[String]): Unit = {
    import akka.pattern.ask
    implicit val timeout:akka.util.Timeout = 30 seconds

    buildOptionParser.parse(args,Options()) match {
      case None=>
        logger.error("You must specify commandline options. Try --help for starters.")
        system.terminate().andThen({
          case _=>sys.exit(3)
        })
      case Some(opts)=>
        UserInfoBuilder.fromFile(opts.vaultFile) match {
          case Failure(err)=>
            logger.error(s"Could not read vault information from ${opts.vaultFile}: ", err)
            system.terminate().andThen({
              case _=>sys.exit(2)
            })
          case Success(userInfo)=>
            logger.info(s"Starting file scan from ${opts.localPath}....")
            val startPathFile = new File(opts.localPath)
            if(!startPathFile.exists()){
              logger.error(s"Could not find the starting path $startPathFile")
              system.terminate().andThen({
                case _=>sys.exit(3)
              })
            } else {
              val fileScanFuture = getFileSystemContent(startPathFile.toPath, opts.excludePathsFile)

              logger.info(s"Starting remote scan from ${opts.vaultFile}...")
              val vaultScanFuture = getObjectMatrixContent(userInfo)

              Future.sequence(Seq(fileScanFuture, vaultScanFuture))
                .flatMap(results => {
                  (estimateActor ? BackupEstimateGroup.QuerySize).mapTo[SizeReturn].flatMap(sizeRtn => {
                    //remote content is held in the actor at estimateActor. This is to quickly access it via maps.
                    val localContentSeq = results.head.asInstanceOf[Seq[BackupEstimateEntry]]
                    logger.info(s"Completed remote and local file scans. Got ${localContentSeq.length} local entries and $sizeRtn remote entries")

                    performCrossMatch(localContentSeq)
                  })
                })
                .onComplete({
                  case Success(finalEstimate) =>
                    logger.info(s"Final estimate results: ${finalEstimate.needsBackupSize} b comprising of ${finalEstimate.needsBackupCount} files need backing up")
                    logger.info(s"${finalEstimate.noBackupSize} b conmprising of ${finalEstimate.noBackupCount} files do not need backing up")
                    system.terminate()
                  case Failure(err) =>
                    logger.error(s"Backup estimate process failed: ", err)
                    system.terminate().andThen({
                      case _ => sys.exit(1)
                    })
                })
            }
        }
    }
  }
}

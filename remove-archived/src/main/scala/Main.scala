import java.io.File

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import akka.stream.{ActorMaterializer, ClosedShape, FlowShape, IOResult, Materializer}
import akka.stream.scaladsl.{Balance, FileIO, GraphDSL, Keep, Merge, RunnableGraph, Sink, Source}
import akka.util.ByteString
import com.om.mxs.client.japi.{Attribute, Constants, MatrixStore, MxsObject, SearchTerm, UserInfo, Vault}
import helpers.TrustStoreHelper
import models.{ArchiveStatus, ObjectMatrixEntry, PotentialRemoveStreamObject}
import org.slf4j.LoggerFactory
import streamcomponents.{ArchiveHunterExists, ArchiveHunterFileSizeSwitch, LocalFileExists, OMDeleteSink, OMFastSearchSource}

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val actorSystem = ActorSystem.create("removearchived")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  def terminate(exitCode:Int) = actorSystem.terminate().andThen({
    case _=>sys.exit(exitCode)
  })

  val archiveHunterBaseUri = sys.env.get("ARCHIVEHUNTER_BASE_URI") match {
    case Some(uri)=>uri
    case None=>
      logger.error("You must specify ARCHIVEHUNTER_BASE_URI in the environment")
      Await.ready(terminate(1), 2 hours)
      throw new RuntimeException("should not get here")
  }

  val archiveHunterKey = sys.env.get("ARCHIVEHUNTER_SECRET_KEY") match {
    case Some(key)=>key
    case None=>
      logger.error("You must specify ARCHIVEHUNTER_SECRET_KEY in the environment")
      Await.ready(terminate(1), 2 hours)
      throw new RuntimeException("should not get here")
  }

  val archiveStripPathElements = sys.env.getOrElse("ARCHIVE_STRIP_PATH","0").toInt
  val paralellism = sys.env.getOrElse("PARALELLISM","8").toInt

  val vaultFile = sys.env.get("VAULT_FILE") match {
    case Some(file)=>file
    case None=>
      logger.error("You must specify VAULT_FILE in the environment")
      Await.ready(terminate(1), 2 hours)
      throw new RuntimeException("should not get here")
  }

  val maybeDeletionReportFile = sys.env.get("DELETION_REPORT_FILE")

  lazy val extraKeyStores = sys.env.get("EXTRA_KEY_STORES").map(_.split("\\s*,\\s*"))

  val reallyDelete = sys.env.get("REALLY_DELETE")

  def buildStream(userInfo:UserInfo) = {
    import akka.stream.scaladsl.GraphDSL.Implicits._
    val includeFields = Seq("MXFS_PATH", "MXFS_FILENAME", "DPSP_SIZE")
    val terms = Array(SearchTerm.createSimpleTerm(Constants.CONTENT, s"*\nkeywords: ${includeFields.mkString(",")}"))
    val sinkFact = Sink.seq[PotentialRemoveStreamObject]

    val processor = GraphDSL.create() { implicit builder =>
      val existsSwitch = builder.add(new LocalFileExists)
      val remoteExistsSwitch = builder.add(new ArchiveHunterExists(archiveHunterBaseUri, archiveHunterKey, archiveStripPathElements))
      val fileSizeSwitch = builder.add(new ArchiveHunterFileSizeSwitch)
      val merger = builder.add(Merge[PotentialRemoveStreamObject](4, false))

      existsSwitch.out(0).map(_.copy(status = Some(ArchiveStatus.SHOULD_KEEP))) ~> merger //YES branch => file does exist on primary, not ready to remove

      existsSwitch.out(1) ~> remoteExistsSwitch //NO branch => file does not exist on primary, we can consider removing it
      remoteExistsSwitch.out(1).map(_.copy(status = Some(ArchiveStatus.NOT_ARCHIVED))) ~> merger //NO branch => file does not exist on primary nor remote

      remoteExistsSwitch.out(0) ~> fileSizeSwitch
      fileSizeSwitch.out(0).map(_.copy(status = Some(ArchiveStatus.SAFE_TO_DELETE))) ~> merger //YES branch => local and remote sizes match, can delete
      fileSizeSwitch.out(1).map(_.copy(status = Some(ArchiveStatus.ARCHIVE_CONFLICT))) ~> merger //NO branch => file sizes differ, not safe to delete
      FlowShape(existsSwitch.in, merger.out)
    }

    GraphDSL.create(sinkFact) { implicit builder =>
      sink =>
        val src = builder.add(new OMFastSearchSource(userInfo, terms, Array(), contentSearchBareTerm = true))
        val splitter = builder.add(Balance[PotentialRemoveStreamObject](paralellism, true))
        val finalMerger = builder.add(Merge[PotentialRemoveStreamObject](paralellism, false))

        src.out.map(PotentialRemoveStreamObject.apply) ~> splitter

        for (i <- 0 until paralellism) {
          val p = builder.add(processor)
          splitter.out(i) ~> p ~> finalMerger
        }
        finalMerger ~> sink
        ClosedShape
    }
  }

  def getMaybeLocalSize(entry:ObjectMatrixEntry):Option[Long] = {
    entry.longAttribute("DPSP_SIZE") match {
      case value @Some(_)=>value
      case None=>
        entry.stringAttribute("DPSP_SIZE").map(_.toLong)
    }
  }

  /**
    * outputs the given sequence of objects as a CSV file
    * @param records
    * @param toFileName
    * @return
    */
  def outputList(records:Seq[PotentialRemoveStreamObject], toFileName:String):Future[IOResult] = {
    val f = new File(toFileName)
    val sink = FileIO.toPath(f.toPath)

    Source
      .fromIterator(()=>records.iterator)
      .map(entry=>"\""+ entry.omFile.pathOrFilename.getOrElse("(no name)")+"\""+s",${getMaybeLocalSize(entry.omFile).getOrElse(-1L)},${entry.archivedSize.getOrElse(-1L)}\n")
      .map(ByteString.apply)
      .toMat(sink)(Keep.right)
      .run()
  }

  def performDeletion(records:Seq[PotentialRemoveStreamObject], userInfo:UserInfo, reallyDelete:Boolean):Future[Done] = {
    val sinkFact = new OMDeleteSink(userInfo, reallyDelete)

    val graph = GraphDSL.create(sinkFact) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val splitter = builder.add(Balance[ObjectMatrixEntry](paralellism, true))

      Source
        .fromIterator(()=>records.filter(_.status.contains(ArchiveStatus.SAFE_TO_DELETE)).toIterator)
        .map(_.omFile) ~> splitter

      for(i <- 0 to paralellism) splitter.out(i) ~> sink

      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }

  def summarise(scanData:Seq[PotentialRemoveStreamObject],forState: ArchiveStatus.Value, outputTo:Option[String]=None):(Int,Long,Option[Future[IOResult]]) = {
    val matchingRecords = scanData.filter(_.status.contains(forState))
    val totalSize = matchingRecords.foldLeft[Long](0L)((acc,elem)=>acc+getMaybeLocalSize(elem.omFile).getOrElse(0L))
    val totalCount = matchingRecords.length

    val maybeCsvFut = outputTo.map(filename=>outputList(matchingRecords, filename))
    (totalCount, totalSize, maybeCsvFut)
  }

  def testConnection(userInfo:UserInfo) =
    Try { MatrixStore.openVault(userInfo) } match {
      case Success(v) =>
        val totalSpace = v.getAttributes.totalSpace()
        logger.info(s"Connected to ${userInfo.getClusterId} on ${userInfo.getAddresses} with $totalSpace bytes total")
        v.dispose()
      case Failure(err) =>
        logger.error(s"Could not connect to vault $userInfo", err)
        sys.exit(3)
    }

  def main(args: Array[String]): Unit = {
    if(extraKeyStores.isDefined){
      logger.info(s"Loading in extra keystores from ${extraKeyStores.get.mkString(",")}")
      /* this should set the default SSL context to use the stores as well */
      TrustStoreHelper.setupTS(extraKeyStores.get) match {
        case Success(sslContext) =>
          val https: HttpsConnectionContext = ConnectionContext.https(sslContext)
          Http().setDefaultClientHttpsContext(https)
        case Failure(err) =>
          logger.error(s"Could not set up https certs: ", err)
          System.exit(1)
      }
    }

    UserInfoBuilder.fromFile(vaultFile) match {
      case Failure(err)=>
        logger.error(s"Could not read vault data from $vaultFile", err)
        sys.exit(2)
      case Success(userInfo)=>
        testConnection(userInfo)
        RunnableGraph.fromGraph(buildStream(userInfo)).run().onComplete({
          case Failure(err)=>
            logger.error(s"Could not perform scan: ", err)
            terminate(3)
          case Success(scanData)=>
            logger.info("Run completed")
            val (canDeleteCount, canDeleteSize, writeCsvFut) = summarise(scanData, ArchiveStatus.SAFE_TO_DELETE, maybeDeletionReportFile)
            val canDeleteSizeGb = canDeleteSize / scala.math.pow(10,9).toLong
            logger.info(s"$canDeleteCount files comprising ${canDeleteSizeGb} Gb can be deleted")
            val (notArchCount, notArchSize, _) = summarise(scanData, ArchiveStatus.NOT_ARCHIVED)
            val notArchSizeGb = notArchSize / scala.math.pow(10,9).toLong
            logger.info(s"$notArchCount files comprising $notArchSizeGb Gb are not archived")
            val (conflictCount, conflictSize, _) = summarise(scanData, ArchiveStatus.ARCHIVE_CONFLICT)
            val conflictSizeGb = conflictSize / scala.math.pow(10,9).toLong
            logger.info(s"$conflictCount files comprising ${conflictSizeGb} Gb exist in archive with the wrong sizes")
            val (keepCount, keepSize, _) = summarise(scanData, ArchiveStatus.SHOULD_KEEP)
            val keepSizeGb = keepSize / scala.math.pow(10,9).toLong
            logger.info(s"$keepCount files comprising ${keepSizeGb} Gb are still present on primary")

            val deleteFut=if(reallyDelete.contains("yes")) {
              logger.info(s"Performing deletion of $canDeleteCount files totalling $canDeleteSizeGb Gb....")
              performDeletion(scanData.filter(_.status.contains(ArchiveStatus.SAFE_TO_DELETE)), userInfo, reallyDelete.contains("yes"))
            } else {
              logger.warn("No deletion is being performed. To delete files, set the environment variable REALLY_DELETE to the exact string 'yes'")
              Future(Done)
            }

            val futureSeq = Seq(writeCsvFut, Some(deleteFut)).collect({case Some(f)=>f})
            Future.sequence(futureSeq).onComplete({
              case Failure(err)=>
                logger.error(s"Either CSV write or deletion failed: ", err)
                terminate(4)
              case Success(_)=>
                logger.info("All done")
                terminate(0)
            })

        })
    }
  }
}

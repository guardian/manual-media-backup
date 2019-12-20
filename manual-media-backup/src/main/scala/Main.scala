import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Path
import java.util.Properties

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, FlowShape, Materializer, SourceShape}
import akka.pattern.ask
import com.om.mxs.client.japi.{Attribute, Constants, MatrixStore, SearchTerm, UserInfo, Vault}
import helpers.PlutoCommunicator.{AFHMsg, LookupFailed, TestConnection}
import models.{CopyReport, IncomingListEntry, ObjectMatrixEntry}
import org.slf4j.LoggerFactory
import streamcomponents.{FilesFilter, ListCopyFile, ListRestoreFile, OMLookupMetadata, OMMetaToIncomingList, OMSearchSource, ProgressMeterAndReport, ValidateMD5}
import helpers.{Copier, ListReader, MatrixStoreHelper, PlutoCommunicator}
import models.{BackupEntry, CopyReport, CustomMXSMetadata, IncomingListEntry, ObjectMatrixEntry}
import org.slf4j.LoggerFactory
import streamcomponents._

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val actorSystem = ActorSystem("objectmatrix-test")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  /**
    * asynchronously shuts down the actorsystem and then terminates the JVM session with the given exitcode
    * @param exitCode exitcode to return to system
    * @return a Future, which should effectively never resolve (JVM should quit as it does)
    */
  def terminate(exitCode:Int) = actorSystem.terminate().andThen({
    case _=>System.exit(exitCode)
  })

  def buildOptionParser = {
    new scopt.OptionParser[Options]("manual-media-backup") {
      head("manual-media-backup", "1.x")

      opt[String]("vault-file").action((x,c)=>c.copy(vaultFile = x)).text(".vault file from ObjectMatrix Admin that describes the cluster, vault and login details. This is provided when you create the vault.")
      opt[String]("lookup").action((x,c)=>c.copy(lookup = Some(x))).text("look up this filepath on the provided ObjectMatrix")
      opt[String]('c',"copy-to-local").action((x,c)=>c.copy(copyToLocal = Some(x))).text("set to a filepath to copy from the OM to a local file")
      opt[String]('f', "copy-from-local").action((x,c)=>c.copy(copyFromLocal = Some(x))).text("set this to copy from a local file onto the OM")
      opt[Int]('s',"chunk-size").action((x,c)=>c.copy(chunkSize = x)).text("set chunk size for transfer in Kb/s")
      opt[String]('t',"checksum-type").action((x,c)=>c.copy(checksumType = x)).text("use the given checksum type (md5, sha-1, sha-256 etc.) or 'none' for no checksum. Defaults to \"md5\", as this is the checksum format used on the MatrixStore.")
      opt[String]('l',"list").action((x,c)=>c.copy(listpath = Some(x))).text("read a list of files to backup from here. This could be a local filepath or an http/https URL.")
      opt[String]('p',"parallelism").action((x,c)=>c.copy(parallelism = x.toInt)).text("copy this many files at once")
      opt[String]("list-path").action((x,c)=>c.copy(listRemoteDirs = Some(x))).text("search the given filename path on the objectmatrix")
      opt[String]("delete-oid").action((x,c)=>c.copy(oidsToDelete = x.split("\\s*,\\s*").toList)).text("Delete file with the given OID")
      opt[String]("pluto-credentials").action((x,c)=>c.copy(plutoCredentialsProperties = Some(x))).text("A .properties file with host and credentials for Pluto, used for metadata extraction")
      opt[String]("path-definitions-file").action((x,c)=>c.copy(pathDefinitionsFile = Some(x))).text("A json file that gives mappings from paths to types")
      opt[String]("report-path").action((x,c)=>c.copy(reportOutputFile = Some(x))).text("Writable path to output a backup report to")
      opt[String]("exclude-paths-file").action((x,c)=>c.copy(excludePathsFile=Some(x))).text("A Json file that gives an array of filepaths to exclude as regexes")
      opt[Boolean]("test-pluto-connection").action((x,c)=>c.copy(testPlutoConnection = true))
      opt[Boolean]("everything").action((x,c)=>c.copy(everything=true)).text("Backup everything at the given path")
    }
  }

  def fullBackupGraph(startingPath:Path,paralellism:Int, userInfo:UserInfo, chunkSize:Int, checksumType:String, plutoCommunicator:ActorRef,pathDefinitionsFile:String,excludeListFile:Option[String]) = {
    val copierFactory = new BatchCopyFile(userInfo,checksumType, chunkSize)
    val commitMetaFactory = new OMCommitMetadata(userInfo)
    val clearBeingWrittenFactory = new ClearBeingWritten(userInfo)
    val getMimeFactory = new GetMimeType
    val checkOMFileFactory = new CheckOMFile(userInfo)
    val createEntryFactory = new CreateOMFileNoCopy(userInfo)
    val needsBackupFactory = new NeedsBackupSwitch
    val addTypeFactory = new AddTypeField(pathDefinitionsFile)
    val gatherMetadataFactory = new GatherMetadata(plutoCommunicator)

    val processingGraph = GraphDSL.create() { implicit builder=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val existSwitch = builder.add(checkOMFileFactory)
      val createEntry = builder.add(createEntryFactory)
      val getMimeType = builder.add(getMimeFactory)
      val needsBackupSwitch = builder.add(needsBackupFactory)
      val fileCheckMerger = builder.add(Merge[BackupEntry](2))

      val gatherMetadata = builder.add(gatherMetadataFactory)
      val addType = builder.add(addTypeFactory)
      val copier = builder.add(copierFactory)
      val omCommitMetadata = builder.add(commitMetaFactory)
      val clearBeingWritten = builder.add(clearBeingWrittenFactory)
      val finalMerger = builder.add(Merge[BackupEntry](2))

      existSwitch.out(0) ~> needsBackupSwitch                     //"yes" branch => given file exists on nearline
      existSwitch.out(1) ~> createEntry ~> fileCheckMerger        //"no"  branch => given file does not exist on nearline

      needsBackupSwitch.out(0) ~> fileCheckMerger                 //"yes" branch => file still needs backup
      needsBackupSwitch.out(1) ~> finalMerger                     //"no" branch => file does not need backup

      //FIXME: check whether we still need omCommitMetadata or not
      fileCheckMerger ~> getMimeType ~> addType ~> gatherMetadata ~> copier ~> clearBeingWritten ~> omCommitMetadata ~> finalMerger
      FlowShape.of(existSwitch.in, finalMerger.out)
    }

    //placeholder, do something better with end result when we know what that is
    val finalSinkFact = Sink.seq[BackupEntry]

    GraphDSL.create(finalSinkFact) {implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val src = builder.add(FileListSource(startingPath))
      val dirFilter = builder.add(new FilterOutDirectories)
      val excludeFilter = builder.add(new ExcludeListSwitch(excludeListFile))
      val pathCharsetConverter = builder.add(new UTF8PathCharset)
      val splitter = builder.add(Balance[BackupEntry](paralellism).async)
      val merger = builder.add(Merge[BackupEntry](paralellism))

      val processorFactory = processingGraph

      src ~> dirFilter ~> pathCharsetConverter  ~> excludeFilter
      excludeFilter.out.map(path=>BackupEntry(path,None)).log("streamcomponents.fullbackupgraph") ~> splitter
      for(i <- 0 until paralellism) {
        val processor = builder.add(processorFactory)
        splitter.out(i) ~> processor ~> merger.in(i)
      }

      merger ~> sink
      ClosedShape
    }
  }

  /**
    * builds a graph that counts how many files need to be backed up in a fill backup
    * @param startingPath java.nio.Path indicating the path which needs to be recursively backed up
    * @param userInfo UserInfo instance indicating the OM appliance and vault which the backup will be performed to
    * @return a Graph that materializes an instance of CounterData. count1 represents the files that need backup and count2 represents the files that don't.
    */
  def fullBackupEstimateGraph(startingPath:Path, userInfo:UserInfo, excludeListFile:Option[String]) = {
    val sinkFac = new TwoPortCounter[BackupEntry]
    val checkOMFileFactory = new CheckOMFile(userInfo)
    val needsBackupFactory = new NeedsBackupSwitch

    GraphDSL.create(sinkFac) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val src = builder.add(FileListSource(startingPath))
      val dirFilter = builder.add(new FilterOutDirectories)
      val excludeFilter = builder.add(new ExcludeListSwitch(excludeListFile))
      val pathCharsetConverter = builder.add(new UTF8PathCharset)
      val existSwitch = builder.add(checkOMFileFactory)
      val needsBackupSwitch = builder.add(needsBackupFactory)
      val needsBackupMerger = builder.add(Merge[BackupEntry](2))

      src ~> dirFilter ~> pathCharsetConverter  ~> excludeFilter
      excludeFilter.out.map(path=>BackupEntry(path)) ~> existSwitch
      existSwitch.out(0) ~> needsBackupSwitch       // "yes" branch => file does exist so check if it needs backup
      existSwitch.out(1) ~> needsBackupMerger         // "no" branch  => file does not exist so it does need backup

      needsBackupSwitch.out(0) ~> needsBackupMerger

      needsBackupMerger.out ~> sink.inlets(0)       //input 0 => needs backup
      needsBackupSwitch.out(1) ~> sink.inlets(1)    //input 1 => does not need backup

      ClosedShape
    }
  }

  def copyToRemoteGraph(fileFilterFactory:FilesFilter, copierFactory:ListCopyFile[Nothing]) = GraphDSL.create() { implicit builder=>
    import akka.stream.scaladsl.GraphDSL.Implicits._
    val checkfile = builder.add(fileFilterFactory)
    val copier = builder.add(copierFactory)
    val merge = builder.add(Merge[CopyReport[Nothing]](2, false))
    checkfile.out(0) ~> copier
    copier ~> merge
    checkfile.out(1).map(entry=>CopyReport(entry.filepath,"",None,0, preExisting = false, validationPassed = None)) ~> merge

    FlowShape(checkfile.in, merge.out)
  }


  def listHandlingGraph(filesList:Seq[IncomingListEntry],paralellism:Int, userInfo:UserInfo, vault:Vault, chunkSize:Int, checksumType:String) = {
    val totalFileSize = filesList.foldLeft(0L)((acc, entry)=>acc+entry.size)

    val sinkFactory = new ProgressMeterAndReport(Some(filesList.length), Some(totalFileSize)).async

    val fileFilterFactory = new FilesFilter(true)
    val copierFactory = new ListCopyFile(userInfo, chunkSize, checksumType,mat)

    GraphDSL.create(sinkFactory) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(Source.fromIterator(()=>filesList.toIterator))
      val splitter = builder.add(Balance[IncomingListEntry](paralellism))
      val merge = builder.add(Merge[CopyReport[Nothing]](paralellism, false))

      src ~> splitter
      for(_ <- 0 until paralellism){
        val copier = builder.add(copyToRemoteGraph(fileFilterFactory, copierFactory).async)
        splitter ~> copier ~> merge
      }
      merge ~> sink
      ClosedShape
    }
  }

  def remoteFileListGraph(paralellism:Int, userInfo:UserInfo, vault:Vault, chunkSize:Int, checksumType:String, searchTerm:String, copyToPath:Option[String]) = {
    val sinkFactory = Sink.fold[Seq[CopyReport[Nothing]],CopyReport[Nothing]](Seq())((acc, entry)=>acc++Seq(entry))
    GraphDSL.create(sinkFactory) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._


      val search = if(searchTerm.length>0){
        new Attribute(Constants.CONTENT, s"""MXFS_FILENAME:"$searchTerm"""" )
      } else {
        new Attribute(Constants.CONTENT, s"*")
      }

      val src = builder.add(new OMSearchSource(userInfo, None, searchAttribute=Some(search)))
      val copierFactory = new ListRestoreFile(userInfo, vault, chunkSize, checksumType, copyToPath)

      if(copyToPath.isDefined){
        val updater = builder.add(new OMLookupMetadata(userInfo))
        val balancer = builder.add(Balance[ObjectMatrixEntry](paralellism))
        val merge = builder.add(Merge[CopyReport[Nothing]](paralellism, false))

        src ~> updater ~> balancer
        for(_ <- 0 until paralellism){
          val copier = builder.add(copierFactory)
          //val validator = builder.add(new ValidateMD5(vault))
          balancer ~> copier ~> merge
        }
        merge ~>sink

      } else {
        val mapper = builder.add(new OMMetaToIncomingList(userInfo, log=true))
        src ~> mapper
        mapper.out.map(entry=>{
          logger.debug(s"got $entry")
          CopyReport(entry.fileName,"",None,entry.size, false, None)
        }) ~> sink

      }
      ClosedShape
    }
  }

  def handleList(listPath:String, userInfo:UserInfo, vault:Vault, chunkSize:Int, checksumType:String, paralellism:Int) = {
    logger.info(s"Loading list from $listPath")
    ListReader.read(listPath, withJson = true).flatMap({
      case Left(err)=>
        logger.error(s"Could not read source list from $listPath: $err")
        Future(Left(err))
      case Right(filesList)=>
        filesList.foreach(entry=>logger.debug(s"$entry"))
        logger.info(s"Completed reading $listPath, got ${filesList.length} entries to back up")
        if(filesList.isInstanceOf[Seq[IncomingListEntry]]){
          val totalSize = filesList.foldLeft[Long](0L)((acc,elem)=>elem.asInstanceOf[IncomingListEntry].size+acc)
          val totalSizeInGb = totalSize.toDouble / 1073741824
          logger.info(s"Total size is $totalSizeInGb Gb")
        }

        RunnableGraph.fromGraph(listHandlingGraph(filesList.map(_.asInstanceOf[IncomingListEntry]), paralellism, userInfo, vault, chunkSize, checksumType)).run().map(result=>Right(result))
    })
  }

  def handleRemoteFileList(paralellism:Int, userInfo:UserInfo, vault:Vault, chunkSize:Int, checksumType:String, searchTerm:String, copyToPath:Option[String]) = {
    RunnableGraph.fromGraph(remoteFileListGraph(paralellism, userInfo, vault, chunkSize, checksumType, searchTerm, copyToPath)).run().map(filesList=>{
      val totalFileSize = filesList.foldLeft(0L)((acc,entry)=>acc+entry.size)

      filesList.foreach(entry=>logger.info(s"\tGot ${entry.filename} of ${entry.size}"))
      logger.info(s"Found a total of ${filesList.length} files occupying a total of $totalFileSize bytes")
    })
  }

  def getPlutoCommunicator(filePath:String):Try[ActorRef] = {
    val propsFile = Try {
      val prop = new Properties()

      val f = new File(filePath)
      val strm = new FileInputStream(f)
      prop.load(strm)
      strm.close()
      prop
    }

    propsFile.flatMap(properties=>{
      val baseUri = Option(properties.getProperty("base-uri"))
      val user = Option(properties.getProperty("user"))
      val passwd = Option(properties.getProperty("password"))

      if(baseUri.isEmpty || user.isEmpty || passwd.isEmpty){
        Failure(new RuntimeException("Invalid properties. You must provide base-uri, user and password properties for pluto access"))
      } else {
        Success(actorSystem.actorOf(Props(new PlutoCommunicator(baseUri.get, user.get, passwd.get))))
      }
    })
  }

  /**
    * write the given sequence of BackupEntry to TSV, for debugging
    * @param entries
    * @return
    */
  def writeBackupEntries(entries:Seq[BackupEntry], filepath:String) = Try {
    val f = new File(filepath)
    val stream = new FileOutputStream(f)

    entries.foreach(entry=>{
      val maybeAttribs = entry.maybeObjectMatrixEntry.flatMap(_.attributes)
      val maybeMeta = maybeAttribs.flatMap(CustomMXSMetadata.fromMxsMetadata)
      val maybeMimeType = maybeAttribs.flatMap(_.stringValues.get("MXFS_MIMETYPE"))
      val strToWrite = s"${entry.originalPath.toString}\t${entry.status}\t${maybeMimeType.getOrElse("(none)")}\t$maybeMeta\n"
      stream.write(strToWrite.toCharArray.map(_.toByte))
    })
    stream.close()
  }

  implicit val timeout:akka.util.Timeout = 30 seconds
  def main(args:Array[String]):Unit = {
    buildOptionParser.parse(args, Options()) match {
      case Some(options)=>
        UserInfoBuilder.fromFile(options.vaultFile) match {
          case Failure(err)=>
            println(s"Could not parse vault file: ")
            err.printStackTrace()
          case Success(userInfo)=>
            val vault = MatrixStore.openVault(userInfo)

            if(options.everything){
              if(options.copyFromLocal.isEmpty){
                logger.error("Can't perform backup when a starting path is not supplied. Use --copy-from-local to specify a starting path")
                terminate(1)
              }
              if(options.plutoCredentialsProperties.isEmpty){
                logger.error("You must provide a pluto credentials property file for this operation.")
                terminate(1)
              }
              val plutoCommunicator:ActorRef = getPlutoCommunicator(options.plutoCredentialsProperties.get) match {
                case Success(comm)=>comm
                case Failure(err)=>
                  logger.error(s"Could not set up pluto communicator: ", err)
                  terminate(2)
                  throw new RuntimeException("This code should not be reachable")
              }

              //blocking here is NOT a sin, because we need to wait for this to complete anyway before other threads kick in
              Await.result((plutoCommunicator ? TestConnection).mapTo[AFHMsg], 30 seconds) match {
                case LookupFailed=>
                  logger.error(s"Could not communicate with pluto, see earlier errors")
                  terminate(3)
                  throw new RuntimeException("This code should not be reachable")
                case _=>
                  logger.info(s"Pluto connection verified")
              }

              val startPath = new File(options.copyFromLocal.get)
              if(!startPath.exists()){
                logger.error(s"Provided starting path ${startPath.toString} does not exist.")
                terminate(2)
              }

              val estimateGraph = fullBackupEstimateGraph(startPath.toPath, userInfo, options.excludePathsFile)
              val actualGraph = fullBackupGraph(startPath.toPath,options.parallelism,userInfo, options.chunkSize*1024,
                options.checksumType, plutoCommunicator, options.pathDefinitionsFile.get, options.excludePathsFile)
              logger.info("Counting total files for backup...")
              val countPromise = RunnableGraph.fromGraph(estimateGraph).run()

              countPromise.future.onComplete({
                case Success(countData)=>
                  logger.info(s"Full backup estimate: ${countData.count1} files need backing up and ${countData.count2} files don't need backing up")
                  logger.info("Starting up copy graph...")
                  val resultFuture = RunnableGraph.fromGraph(actualGraph).run()

                  resultFuture.onComplete({
                    case Success(backupEntrySeq)=>
                      val writeResult = options.reportOutputFile.map(filepath=>writeBackupEntries(backupEntrySeq, filepath)).getOrElse(Success(()))
                      writeResult match {
                        case Failure(err)=>
                          logger.error(s"Could not output test dump: ", err)
                          terminate(2)
                        case Success(_)=>
                          logger.info("All done")
                          terminate(0)
                      }
                    case Failure(err)=>
                      logger.error("Could not perform full backup : ", err)
                      terminate(2)
                  })
                case Failure(err)=>
                  logger.error(s"Could not count files to back up: ",err)
              })

            } else if(options.listpath.isDefined) {
              handleList(options.listpath.get, userInfo, vault, options.chunkSize, options.checksumType, options.parallelism).andThen({
                case Success(Right(finalReport)) =>
                  logger.info("All operations completed")
                  terminate(0)
                case Success(Left(err)) =>
                  logger.error(s"Could not start backup operation: $err")
                  terminate(1)
                case Failure(err) =>
                  logger.error(s"Uncaught exception: ", err)
                  terminate(1)
              })
            } else if(options.listRemoteDirs.isDefined){
              handleRemoteFileList(options.parallelism, userInfo, vault, options.chunkSize, options.checksumType, options.listRemoteDirs.get, options.copyToLocal).andThen({
                case Success(_)=>
                  logger.info("All operations completed")
                  terminate(0)
                case Failure(err)=>
                  logger.error(s"Uncaught exception: ", err)
                  terminate(1)
              })
            } else if(options.copyFromLocal.isDefined){
              Copier.copyFromLocal(userInfo, vault, options.lookup, options.copyFromLocal.get, options.chunkSize*1024, options.checksumType).andThen({
                case Success(_)=>
                  logger.info(s"All operations completed")
                  terminate(0)
                case Failure(err)=>
                  logger.error("",err)
                  terminate(1)
              })
            } else if(options.lookup.isDefined){
              Copier.lookupFileName(userInfo, vault, options.lookup.get, options.copyToLocal).onComplete({
                case Success(_)=>
                  logger.info(s"All operations completed")
                  terminate(0)
                case Failure(err)=>
                  println(err.toString)
                  terminate(1)
              })
            } else if(options.oidsToDelete.nonEmpty){
              options.oidsToDelete.foreach(oid=> {
                MatrixStoreHelper.deleteFile(vault, oid) match {
                  case Success(_)=>logger.info(s"Successfully deleted file with OID $oid")
                  case Failure(err)=>logger.warn(s"Could not delete file with OID $oid: ", err)
                }
              })
              terminate(0)
            }
        }
      case None=>
        // arguments are bad, error message will have been displayed
        terminate(1)
    }

  }
}

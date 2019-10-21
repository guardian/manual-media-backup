import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, Broadcast, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, FlowShape, Materializer, SourceShape}
import com.om.mxs.client.SimpleSearchTerm
import com.om.mxs.client.japi.{Attribute, Constants, MatrixStore, SearchTerm, UserInfo, Vault}
import helpers.{Copier, ListReader, MatrixStoreHelper}
import models.{CopyReport, IncomingListEntry, ObjectMatrixEntry}
import org.slf4j.LoggerFactory
import streamcomponents.{FilesFilter, ListCopyFile, ListRestoreFile, OMLookupMetadata, OMMetaToIncomingList, OMSearchSource, ProgressMeterAndReport, ValidateMD5}

import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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
    val copierFactory = new ListCopyFile(userInfo, vault,chunkSize, checksumType,mat)

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

  def main(args:Array[String]):Unit = {
    buildOptionParser.parse(args, Options()) match {
      case Some(options)=>
        UserInfoBuilder.fromFile(options.vaultFile) match {
          case Failure(err)=>
            println(s"Could not parse vault file: ")
            err.printStackTrace()
          case Success(userInfo)=>
            val vault = MatrixStore.openVault(userInfo)

            if(options.listpath.isDefined) {
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

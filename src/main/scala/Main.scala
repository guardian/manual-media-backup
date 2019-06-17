import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, FileIO, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import com.sun.javaws.progress.Progress
import helpers.{Copier, ListReader, MatrixStoreHelper, MetadataHelper}
import models.{CopyReport, Incoming, IncomingListEntry, MxsMetadata, ObjectMatrixEntry}
import org.slf4j.LoggerFactory
import streamcomponents.{ListCopyFile, ProgressMeterAndReport}

import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
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
      opt[String]('t',"checksum-type").action((x,c)=>c.copy(checksumType = x)).text("use the given checksum type (md5, sha-1, sha-256 etc.) or 'none' for no checksum")
      opt[String]('l',"list").action((x,c)=>c.copy(listpath = Some(x))).text("read a list of files to backup from here. This could be a local filepath or an http/https URL.")
    }
  }

  def listHandlingGraph(filesList:Seq[IncomingListEntry],paralellism:Int, userInfo:UserInfo, vault:Vault, chunkSize:Int, checksumType:String) = {
    val totalFileSize = filesList.foldLeft(0L)((acc, entry)=>acc+entry.size)

    val sinkFactory = new ProgressMeterAndReport(Some(filesList.length), Some(totalFileSize)).async

    GraphDSL.create(sinkFactory) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(Source.fromIterator(()=>filesList.toIterator))
      val splitter = builder.add(Broadcast[IncomingListEntry](paralellism, true))
      val merge = builder.add(Merge[CopyReport](paralellism, false))

      src ~> splitter
      for(_ <- 0 until paralellism){
        val copier = builder.add(new ListCopyFile(userInfo, vault,chunkSize, checksumType,mat).async)
        splitter ~> copier
        copier ~> merge
      }
      merge ~> sink
      ClosedShape
    }
  }

  def handleList(listPath:String, userInfo:UserInfo, vault:Vault, chunkSize:Int, checksumType:String, paralellism:Int) = {
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

  def main(args:Array[String]):Unit = {
    buildOptionParser.parse(args, Options()) match {
      case Some(options)=>
        UserInfoBuilder.fromFile(options.vaultFile) match {
          case Failure(err)=>
            println(s"Could not parse vault file: ")
            err.printStackTrace()
          case Success(userInfo)=>
            val vault = MatrixStore.openVault(userInfo)

            if(options.listpath.isDefined){
              handleList(options.listpath.get, userInfo, vault,options.chunkSize, options.checksumType, 4).andThen({
                case Success(Right(finalReport))=>
                  logger.info("All operations completed")
                  terminate(0)
                case Success(Left(err))=>
                  logger.error(s"Could not start backup operation: $err")
                  terminate(1)
                case Failure(err)=>
                  logger.error(s"Uncaught exception: ", err)
                  terminate(1)
              })
            } else if(options.copyFromLocal.isDefined){
              Await.ready(Copier.copyFromLocal(userInfo, vault, options.lookup, options.copyFromLocal.get, options.chunkSize*1024, options.checksumType).andThen({
                case Success(_)=>terminate(0)
                case Failure(err)=>
                  logger.error("",err)
                  terminate(1)
              }), 3 hours)
              logger.info(s"All operations completed")
            } else if(options.lookup.isDefined){
              Copier.lookupFileName(userInfo, vault, options.lookup.get, options.copyToLocal) match {
                case Success(completedFuture)=>
                  Await.ready(completedFuture, 60 seconds)
                  logger.info(s"All operations completed")
                case Failure(err)=>
                  println(err.toString)
                  terminate(1)
              }
            }
        }

        //terminate(0)
      case None=>
        // arguments are bad, error message will have been displayed
        terminate(1)
    }

  }
}

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, FileIO, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import helpers.{Copier, ListReader, MatrixStoreHelper, MetadataHelper}
import models.{IncomingListEntry, MxsMetadata, ObjectMatrixEntry}
import org.slf4j.LoggerFactory
import streamcomponents.{ChecksumSink, ListCopyFile, MMappedFileSource, MatrixStoreFileSink, MatrixStoreFileSource}

import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.JavaConverters._

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


  def handleList(listPath:String, userInfo:UserInfo, vault:Vault, chunkSize:Int, checksumType:String, paralellism:Int) = {
    ListReader.read(listPath, withJson=true).flatMap({
      case Left(err)=>
        logger.error(s"Could not read source list from $listPath: $err")
        Future(Left(err))
      case Right(filesListRaw)=>
        val filesList = filesListRaw.asInstanceOf[Seq[IncomingListEntry]] //nasty, just here for testing
        filesList.foreach(entry=>logger.debug(entry.toString))
        logger.info(s"Completed reading $listPath, got ${filesList.length} entries to back up")
        val sinkFactory = Sink.ignore
        Future(Right("ok"))
//        val graph = GraphDSL.create(sinkFactory) { implicit builder=> sink=>
//          import akka.stream.scaladsl.GraphDSL.Implicits._
//
//          val src = builder.add(Source.fromIterator(()=>filesList.toIterator))
//          val splitter = builder.add(Broadcast[IncomingListEntry](paralellism, true))
//          val merge = builder.add(Merge[IncomingListEntry](paralellism, false))
//
//          src ~> splitter
//          for(_ <- 0 until paralellism){
//            val copier = builder.add(new ListCopyFile(userInfo, vault,chunkSize, checksumType,mat).async)
//            splitter ~> copier
//            copier ~> merge
//          }
//          merge ~> sink
//          ClosedShape
//        }

//        RunnableGraph.fromGraph(graph).run().map(result=>Right(result))
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

            if(options.copyFromLocal.isDefined){
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

        terminate(0)
      case None=>
        // arguments are bad, error message will have been displayed
        terminate(1)
    }

  }
}

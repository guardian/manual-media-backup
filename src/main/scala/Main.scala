import java.io.File
import java.nio.file.Path

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, FileIO, GraphDSL, RunnableGraph}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.util.ByteString
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import helpers.{MatrixStoreHelper, MetadataHelper}
import models.ObjectMatrixEntry
import org.slf4j.LoggerFactory
import streamcomponents.{ChecksumSink, MatrixStoreFileSource}

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object Main {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val actorSystem = ActorSystem("objectmatrix-test")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  def terminate(exitCode:Int) = actorSystem.terminate().andThen({
    case _=>System.exit(exitCode)
  })

  def buildOptionParser = {
    new scopt.OptionParser[Options]("manual-media-backup") {
      head("manual-media-backup", "1.x")

      opt[String]("vault-file").action((x,c)=>c.copy(vaultFile = x)).text(".vault file from ObjectMatrix Admin that describes the cluster, vault and login details. This is provided when you create the vault.")
      opt[String]('l',"lookup").action((x,c)=>c.copy(lookup = Some(x))).text("look up this filepath on the provided ObjectMatrix")
      opt[String]('c',"local-copy").action((x,c)=>c.copy(copyToLocal = Some(x))).text("set to a filepath to copy to a local file")
    }
  }

  /**
    * stream the file to a local filepath
    * @param entry [[ObjectMatrixEntry]] object representing the file to read from
    * @param toPath java.nio.Path object representing the file to write to. This will be over-written if it exists already.
    * @return a Future, containing a String of the checksum of the read data. If the stream fails then the future will fail, use .recover to handle this.
    */
  def doCopy(userInfo:UserInfo, entry:ObjectMatrixEntry, toPath:Path) = {
    val checksumSinkFactory = new ChecksumSink()

    logger.info("starting doCopy")
    val graph = GraphDSL.create(checksumSinkFactory) { implicit builder=> checksumSink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new MatrixStoreFileSource(userInfo, entry.oid))
      val bcast = builder.add(new Broadcast[ByteString](2,true))
      val fileSink = builder.add(FileIO.toPath(toPath))

      src.out.log("copyStream") ~> bcast ~> fileSink
      bcast.out(1) ~> checksumSink
      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }

  def lookupFileName(userInfo:UserInfo, vault:Vault, fileName: String, copyTo:Option[String]) = {
    MatrixStoreHelper.findByFilename(vault, fileName).map(results=>{
      println(s"Found ${results.length} files: ")

//      val completionFutureList = Future.sequence(results.map(entry=> {
//        entry.getMetadata.andThen({
//          case Success(updatedEntry)=>
//            println(updatedEntry.toString)
//          case Failure(err)=>
//            println(s"Could not get information: $err")
//        })
//      }))

      if(copyTo.isDefined && results.nonEmpty){
        val copyToPath = new File(copyTo.get).toPath
        logger.info(s"copyToPath is $copyToPath")
        //completionFutureList.map(_=>{
          doCopy(userInfo, results.head, copyToPath).andThen({
            case Success(checksum)=>logger.info(s"Completed file copy, checksum was $checksum")
            case Failure(err)=>logger.error(s"Could not copy: ", err)
          })
        //})
      } else {
        Future.successful(())
      }

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

            if(options.lookup.isDefined){
              lookupFileName(userInfo, vault, options.lookup.get, options.copyToLocal) match {
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

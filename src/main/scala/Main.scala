import java.io.File
import java.nio.file.Path

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, FileIO, GraphDSL, RunnableGraph}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.util.ByteString
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import helpers.{MatrixStoreHelper, MetadataHelper}
import models.{MxsMetadata, ObjectMatrixEntry}
import org.slf4j.LoggerFactory
import streamcomponents.{ChecksumSink, MMappedFileSource, MatrixStoreFileSink, MatrixStoreFileSource}

import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
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
      opt[String]('l',"lookup").action((x,c)=>c.copy(lookup = Some(x))).text("look up this filepath on the provided ObjectMatrix")
      opt[String]('c',"copy-to-local").action((x,c)=>c.copy(copyToLocal = Some(x))).text("set to a filepath to copy from the OM to a local file")
      opt[String]('f', "copy-from-local").action((x,c)=>c.copy(copyFromLocal = Some(x))).text("set this to copy from a local file onto the OM")
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

  /**
    * stream a file from the local filesystem into objectmatrix, creating metadata from what is provided by the filesystem.
    * also, performs an SHA-256 checksum on the data as it is copied and sets this in the object's metadata too.
    * @param vault `vault` object indicating where the file is to be stored
    * @param destFileName destination file name. this is checked beforehand, if it exists then no new file will be copied
    * @param fromFile java.nio.File indicating the file to copy from
    * @return a Future, with a string of the final
    */
  def doCopyTo(vault:Vault, destFileName:Option[String], fromFile:File) = {
    val checksumSinkFactory = new ChecksumSink("sha-256").async
    val metadata = MatrixStoreHelper.metadataFromFilesystem(fromFile)

    if(metadata.isFailure){
      Future.failed(metadata.failed.get) //since the stream future fails on error, might as well do the same here.
    } else {
      try {
        val mdToWrite = destFileName match {
          case Some(fn) => metadata.get
              .withString("MXFS_PATH",fromFile.getAbsolutePath)
              .withString("MXFS_FILENAME", fromFile.getName)
              .withString("MXFS_FILENAME_UPPER", fromFile.getName.toUpperCase)
          case None => metadata.get
        }

        val mxsFile = vault.createObject(mdToWrite.toAttributes.toArray)
        val graph = GraphDSL.create(checksumSinkFactory) { implicit builder =>
          checksumSink =>
            import akka.stream.scaladsl.GraphDSL.Implicits._

            val src = builder.add(new MMappedFileSource(fromFile))
            val bcast = builder.add(new Broadcast[ByteString](2, true))
            val omSink = builder.add(new MatrixStoreFileSink(mxsFile).async)

            src ~> bcast ~> omSink
            bcast.out(1) ~> checksumSink
            ClosedShape
        }
        RunnableGraph.fromGraph(graph).run().map(finalChecksum=>{
          val updatedMetadata = metadata.get.copy(stringValues = metadata.get.stringValues ++ Map("SHA-256"->finalChecksum))
          MetadataHelper.setAttributeMetadata(mxsFile, updatedMetadata)
          finalChecksum
        })
      } catch {
        case err:Throwable=>
          Future.failed(err)
      }
    }
  }

  def copyFromLocal(userInfo: UserInfo, vault: Vault, destFileName: Option[String], copyTo: String) = {
    val check = Try { destFileName.flatMap(actualFileame=>MatrixStoreHelper.findByFilename(vault, actualFileame).map(_.headOption).get) }

    check match {
      case Failure(err)=>
        logger.error(s"Could not check for existence of remote file at ${destFileName.getOrElse("(none)")}", err)
        Future.failed(err)
      case Success(Some(existingFile))=>
        logger.error(s"Won't over-write pre-existing file: $existingFile")
        Future.failed(new RuntimeException(s"Won't over-write pre-existing file: $existingFile"))
      case Success(None)=>
        doCopyTo(vault, destFileName, new File(copyTo))
      }
    }


  def lookupFileName(userInfo:UserInfo, vault:Vault, fileName: String, copyTo:Option[String]) = {
    MatrixStoreHelper.findByFilename(vault, fileName).map(results=>{
      println(s"Found ${results.length} files: ")

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

            if(options.copyFromLocal.isDefined){
              copyFromLocal(userInfo, vault, options.lookup, options.copyFromLocal.get)
            } else if(options.lookup.isDefined){
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

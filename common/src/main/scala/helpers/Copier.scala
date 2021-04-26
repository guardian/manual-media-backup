package helpers

import java.io.File
import java.nio.file.Path
import java.time.Instant

import akka.stream.{ClosedShape, Materializer}
import akka.stream.scaladsl.{Broadcast, FileIO, GraphDSL, RunnableGraph, Sink}
import akka.util.ByteString
import com.om.mxs.client.japi.{MatrixStore, MxsObject, UserInfo, Vault}
import models.{CopyProblem, MxsMetadata, ObjectMatrixEntry}
import org.slf4j.LoggerFactory
import streamcomponents.{ChecksumSink, MMappedFileSource, MatrixStoreFileSink, MatrixStoreFileSource}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import org.apache.commons.io.FilenameUtils

object Copier {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * stream the file to a local filepath
    * @param entry [[ObjectMatrixEntry]] object representing the file to read from
    * @param toPath java.nio.Path object representing the file to write to. This will be over-written if it exists already.
    * @return a Future, containing a String of the checksum of the read data. If the stream fails then the future will fail, use .recover to handle this.
    */
  def doCopy(userInfo:UserInfo, entry:ObjectMatrixEntry, toPath:Path)(implicit ec:ExecutionContext,mat:Materializer) = {
    val checksumSinkFactory = new ChecksumSink().async

    logger.info("starting doCopy")
    val graph = GraphDSL.create(checksumSinkFactory) { implicit builder=> checksumSink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new MatrixStoreFileSource(userInfo, entry.oid).async)
      val bcast = builder.add(new Broadcast[ByteString](2,true))
      val fileSink = builder.add(FileIO.toPath(toPath))

      src.out.log("copyStream") ~> bcast ~> fileSink
      bcast.out(1) ~> checksumSink
      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }

  def createObjectWithMetadata(destFileName:Option[String],fromFile:File,metadata:MxsMetadata)(implicit vault:Vault) = Try {
    val mdToWrite = destFileName match {
      case Some(fn) => metadata
        .withString("MXFS_PATH",fromFile.getAbsolutePath)
        .withString("MXFS_FILENAME", fn)
        .withString("MXFS_FILENAME_UPPER", fn.toUpperCase)
      case None => metadata
    }

    logger.debug(s"mdToWrite is $mdToWrite")
    val mxsFile = vault.createObject(mdToWrite.toAttributes().toArray)

    (mxsFile, mdToWrite)
  }

  def createCopyGraph(fromFile:File, chunkSize:Int, checksumType:String, mxsFile:MxsObject)(implicit ec:ExecutionContext) = {
    val checksumSinkFactory = checksumType match {
      case "none"=>Sink.ignore.mapMaterializedValue(_=>Future(None))
      case _=>new ChecksumSink(checksumType)
    }

    GraphDSL.create(checksumSinkFactory) { implicit builder =>
      checksumSink =>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(FileIO.fromPath(fromFile.toPath, chunkSize))
        val bcast = builder.add(new Broadcast[ByteString](2, false).async)
        val omSink = builder.add(new MatrixStoreFileSink(mxsFile))

        src.out.log("copyToStream") ~> bcast ~> omSink
        bcast.out(1) ~> checksumSink
        ClosedShape
    }
  }

  def validateChecksum(mxsFile:MxsObject, actualChecksum:String, keepOnFailure:Boolean)(implicit ec:ExecutionContext) =  MatrixStoreHelper.getOMFileMd5(mxsFile).flatMap({
    case Failure(err) =>
      logger.error(s"Unable to get checksum from appliance, file should be considered unsafe", err)
      Future.failed(err)
    case Success(remoteChecksum) =>
      logger.info(s"Appliance reported checksum of $remoteChecksum")
      if (remoteChecksum != actualChecksum) {
        logger.error(s"Checksum did not match!")
        if (!keepOnFailure) {
          logger.info(s"Deleting invalid file ${mxsFile.getId}")
          mxsFile.deleteForcefully()
        }
        Future.failed(new RuntimeException(s"Checksum did not match"))
      } else {
        Future((mxsFile.getId, Some(actualChecksum)))
      }
  })

  /**
    * stream a file from the local filesystem into objectmatrix, creating metadata from what is provided by the filesystem.
    * also, performs a checksum on the data as it is copied and sets this in the object's metadata too.
    * @param vault `vault` object indicating where the file is to be stored
    * @param destFileName destination file name. this is checked beforehand, if it exists then no new file will be copied
    * @param fromFile java.nio.File indicating the file to copy from
    * @param chunkSize chunk size when streaming the file.
    * @param checksumType checksum type. This must be one of the digest IDs supported by java MessageDigest.
    * @param keepOnFailure boolean, if true then even if a checksum does not match the destination file is kept.
    *                      Defaults to false, delete destination file if checksum does not match.
    * @param retryOnFailure boolean, if true then try again if the checksum does not match. Defaults to true
    * @param ec implicitly provided execution context
    * @param mat implicitly provided materializer
    * @return a Future, with a tuple of (object ID, checksum)
    */
  def doCopyTo(vault:Vault, destFileName:Option[String], fromFile:File, chunkSize:Int, checksumType:String, keepOnFailure:Boolean=false,retryOnFailure:Boolean=true, targetMetadata:Option[MxsMetadata]=None)(implicit ec:ExecutionContext,mat:Materializer):Future[(String,Option[String])] = {
    val metadata = MatrixStoreHelper.metadataFromFilesystem(fromFile)

    if(metadata.isFailure){
      logger.error(s"Could no lookup metadata")
      Future.failed(metadata.failed.get) //since the stream future fails on error, might as well do the same here.
    } else {
      val mdToWrite = targetMetadata match {
        case Some(externalMeta)=>
          metadata.get.merged(externalMeta)
        case None=>metadata.get
      }
      logger.debug(s"Metadata to write is $mdToWrite")

      Future.fromTry(createObjectWithMetadata(destFileName,fromFile, mdToWrite)(vault)).flatMap(result=> {
        val mxsFile = result._1
        val mdToWrite = result._2
        val timestampStart = Instant.now.toEpochMilli
        val graph = createCopyGraph(fromFile, chunkSize,checksumType,mxsFile)

        logger.debug(s"Created stream")
        RunnableGraph.fromGraph(graph).run().flatMap(finalChecksum => {
          val timestampFinish = Instant.now.toEpochMilli
          val msDuration = timestampFinish - timestampStart

          val rate = fromFile.length().toDouble / msDuration.toDouble //in bytes/ms
          val mbps = rate / 1048576 * 1000 //in MByte/s

          val fileNameForOutput = destFileName.getOrElse(fromFile.getPath)
          logger.info(s"$fileNameForOutput: Stream completed, transferred ${fromFile.length} bytes in $msDuration millisec, at a rate of $mbps mByte/s.  Final checksum is $finalChecksum")
          finalChecksum match {
            case Some(actualChecksum) =>
              val updatedMetadata = mdToWrite.copy(stringValues = mdToWrite.stringValues ++ Map(checksumType -> actualChecksum))
              MetadataHelper.setAttributeMetadata(mxsFile, updatedMetadata)

              logger.debug(s"mdToWrite is $updatedMetadata")

              validateChecksum(mxsFile, actualChecksum, keepOnFailure).recoverWith({
                case ex:RuntimeException=>
                  logger.error("Got a runtime exception while validating checksum, retrying: ", ex)
                  if (retryOnFailure) {
                    Thread.sleep(500)
                    doCopyTo(vault, destFileName, fromFile, chunkSize, checksumType, keepOnFailure, retryOnFailure)
                  } else {
                    Future.failed(ex)
                  }
              })

            case None=>
              Future((mxsFile.getId, finalChecksum))
          }
        })
      })
    }
  }

  def copyFromLocal(userInfo: UserInfo, vault: Vault, destFileName: Option[String], localFile: String, chunkSize:Int, checksumType:String)(implicit ec:ExecutionContext, mat:Materializer) = {
    logger.debug("in copyFromLocal")
    val check = Try { destFileName.flatMap(actualFileame=>MatrixStoreHelper.findByFilename(vault, actualFileame, Seq()).map(_.headOption).get) }

    check match {
      case Failure(err)=>
        logger.error(s"Could not check for existence of remote file at ${destFileName.getOrElse("(none)")}", err)
        Future.failed(err)
      case Success(Some(existingFile))=>
        logger.error(s"Won't over-write pre-existing file: $existingFile")
        Future(Left(CopyProblem(existingFile, "File already existed")))
      case Success(None)=>
        logger.debug("Initiating copy")
        doCopyTo(vault, destFileName, new File(localFile), chunkSize, checksumType).map(Right(_))
    }
  }

  def ensurePathExists(pathName:String) = {
    val pathPart = new File(FilenameUtils.getFullPathNoEndSeparator(pathName))
    logger.info(s"creating directories $pathPart")
    pathPart.mkdirs()
  }

  /**
    * returns true if the file does not exist or is zero-length, and should be overwritten
    * @param filePath path to check
    * @return bolean
    */
  def isAbsentOrZerolength(filePath:String) = {
    val f = new File(filePath)
    !f.exists() && f.length()==0
  }

  /**
    * removes a leading slash from a filepath, if present
    * @param from
    * @return
    */
  def removeLeadingSlash(from:String) = {
    if(from.startsWith("/")){
      from.substring(1)
    } else {
      from
    }
  }

  def copyFromRemote(userInfo: UserInfo, vault:Vault, destFileName: Option[String], restorePath:String, remoteFile:ObjectMatrixEntry, chunkSize:Int, checksumType:String)(implicit ec:ExecutionContext, mat:Materializer) = {
    logger.debug("in copyFromRemote")

    val alternativePathLocations = List("MXFS_PATH","MXFS_FILENAME")

    def tryNextLocation(list:List[String]):Option[String] = {
      if(list.isEmpty) return None

      val current = list.head
      remoteFile.stringAttribute(current) match {
        case Some(str)=>Some(str)
        case None=>tryNextLocation(list.tail)
      }
    }

    val maybeFilePath = destFileName match {
      case None=> tryNextLocation(alternativePathLocations)
      case ok @Some(_)=>ok
    }


    maybeFilePath match {
      case None=>
        logger.error(s"Could not find any file path to copy file to")
        Future(Left(CopyProblem(remoteFile,"Could not find any file path to copy file to")))
      case Some(actualFilePath)=>
        val outputFilePath = new File(restorePath, actualFilePath).getPath
        logger.info(s"Copying to $outputFilePath")
        if(!isAbsentOrZerolength(outputFilePath)){
          logger.warn("File already exists, not overwriting")
          Future(Left(CopyProblem(remoteFile,"File already exists locally, not overwriting")))
        } else {
          ensurePathExists(outputFilePath)
          doCopy(userInfo, remoteFile, new File(outputFilePath).toPath).map(maybeCs => Right((actualFilePath, maybeCs)))
        }
    }
  }

  def lookupFileName(userInfo:UserInfo, vault:Vault, fileName: String, copyTo:Option[String])(implicit ec:ExecutionContext, mat:Materializer) = {
    implicit val vaultImpl = vault
    val result = MatrixStoreHelper.findByFilename(vault, fileName, Seq()).map(_.map(_.getMetadata)).map(futureResults=>{

      Future.sequence(futureResults).map(results=> {
        println(s"Found ${results.length} files: ")

        Future.sequence(results.map(entry => {
          println(entry)
          val f = vault.getObject(entry.oid)
          MatrixStoreHelper.getOMFileMd5(f).map({
            case Success(md5) =>
              println(s"File checksum is $md5")
            case Failure(err) =>
              println(s"Could not get checksum: $err")
          })
        }))
      })
    })

    result match {
      case Failure(err)=>Future.failed(err)
      case Success(futures)=>Future.successful( () )
    }
  }

}

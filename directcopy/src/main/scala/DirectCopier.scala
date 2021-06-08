import akka.actor.ActorSystem
import akka.stream.Materializer
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import helpers.{Copier, MatrixStoreHelper}
import models.{MxsMetadata, PathTransform, PathTransformSet, ToCopy}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

object DirectCopier {
  /**
    * creates a [[DirectCopier]] instance pointing to the given vault
    * @param destVaultInfo UserInfo object indicating the vault to open
    * @return either a Success with the given Copier initialised or a Failure indicating the problem
    */
  def initialise(destVaultConnector:MXSConnectionBuilder, maybePathTransform:PathTransformSet) = {
    destVaultConnector.build().flatMap(mxs=>Try {
      val vault = mxs.openVault(destVaultConnector.vaultId)
      new DirectCopier(vault, maybePathTransform)
    })
  }
}

class DirectCopier(destVault:Vault, maybePathTransformList:PathTransformSet) {
  private val logger = LoggerFactory.getLogger(getClass)
  val defaultChunkSize:Int = 5*(1024*1024)

  override def finalize(): Unit = {
    destVault.dispose()
    super.finalize()
  }
  /**
    * calls the static Copier object to perform an individual file copy.
    * Abstracted to its own protected method here for easy mocking in tests
    * @return
    */
  protected def doCopyTo(vault:Vault,
                         destFileName:Option[String],
                         fromFile:File,
                         chunkSize:Int,
                         gnmTypeMetadata:String,
                         checksumType:String,
                         keepOnFailure:Boolean=false,
                         retryOnFailure:Boolean=true,
                         externalMetadata:Option[MxsMetadata])
                        (implicit ec:ExecutionContext,mat:Materializer):Future[(String,Option[String])] = {
    val metaToWrite = externalMetadata
      .getOrElse(MxsMetadata.empty())
      .withString("GNM_TYPE", gnmTypeMetadata)

    Copier.doCopyTo(vault, destFileName, fromFile, chunkSize, checksumType, keepOnFailure, retryOnFailure, Some(metaToWrite))
  }

  def addCopiedOIDs(mediaResult:Future[Seq[(String, Option[String])]], from:ToCopy)(implicit ec:ExecutionContext) = mediaResult.map(results=>{
    logger.debug(s"Copying completed.  Results are $mediaResult")
    val resultCount = results.length
    val updatedSourceFile = from.sourceFile.withCopyData(results.head)

    //not the most elegant solution but it'll do for now I guess
    val updatedProxyMedia = from.proxyMedia.flatMap(proxyMedia=>
      if(resultCount>1) {
        Some(proxyMedia.withCopyData(results(1)))
      } else {
        None
      }
    )

    val updatedThumbnail = from.thumbnail.flatMap(thumb=>
      if(resultCount>2) {
        Some(thumb.withCopyData(results(2)))
      } else if(from.thumbnail.isDefined && resultCount>1) {
        Some(thumb.withCopyData(results(1)))
      } else {
        None
      }
    )

    logger.debug(s"updated source file is $updatedSourceFile")
    logger.debug(s"updated proxy media is $updatedProxyMedia")
    logger.debug(s"updated thumbnail is $updatedThumbnail")

    from.copy(
      sourceFile = updatedSourceFile,
      proxyMedia = updatedProxyMedia,
      thumbnail = updatedThumbnail
    )
  })

  /**
    * call out to MatrixStoreHelper to see if the given path already exists on the remote side
    * @param remotePath path to check
    * @return a Try containing True if the file exists, False if not or a Failure if there was an error
    */
  def checkFileExistence(remotePath:Path) =
    MatrixStoreHelper
      .findByFilename(destVault, remotePath.toString, Seq())
      .map(_.nonEmpty)

  /**
    * performs the copy of all listed files in the [[ToCopy]] structure provided.
    * intended to be called from a mapAsync() stage in the stream.
    * @param from a [[ToCopy]] object indicating what should be copied
    * @return a Future contining an updated [[ToCopy]] object
    */
  def performCopy(from:ToCopy, copyChunkSize:Option[Int]=None)(implicit ec:ExecutionContext, actorSystem:ActorSystem, mat:Materializer) = {
    val itemsToCopy:Seq[(Path,String)] = Seq(
      (Some(from.sourceFile.path), "Rushes"),
      (from.proxyMedia.map(_.path), "Proxy"),
      (from.thumbnail.map(_.path), "Poster"),
    ).collect({case (Some(path), gnmType)=>(path, gnmType)})

    logger.debug(s"Will copy ${itemsToCopy.length} items: ${itemsToCopy.map(_.toString).mkString(",")}")
    val mediaResult = Future.sequence(
      itemsToCopy.map(pathAndType=> {
        val filePath = pathAndType._1
        val gnmType = pathAndType._2

        def conditionalCopy(maybeTransformedPath:Option[Path], alreadyExists:Boolean) = if(!alreadyExists) {
          doCopyTo(destVault,
            maybeTransformedPath.map(_.toString),
            filePath.toFile,
            copyChunkSize.getOrElse(defaultChunkSize),
            gnmTypeMetadata = gnmType,
            checksumType = "md5",
            retryOnFailure = false,
            externalMetadata = from.commonMetadata)
        } else {
          logger.debug(s"File ${maybeTransformedPath.map(_.toString).getOrElse(filePath.toString)} already existed, not copying")
          Future(("",None))
        }

        for {
          maybeTransformedPath <- Future.fromTry(maybePathTransformList.apply(filePath))
          remoteAlreadyExists <- Future.fromTry(checkFileExistence(maybeTransformedPath.getOrElse(filePath)))
          copyResult <- conditionalCopy(maybeTransformedPath, remoteAlreadyExists)
        } yield copyResult
      })
    )

    addCopiedOIDs(mediaResult, from)
  }
}

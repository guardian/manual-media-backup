import akka.actor.ActorSystem
import akka.stream.Materializer
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import helpers.Copier
import models.ToCopy
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object DirectCopier {
  /**
    * creates a [[DirectCopier]] instance pointing to the given vault
    * @param destVaultInfo UserInfo object indicating the vault to open
    * @return either a Success with the given Copier initialised or a Failure indicating the problem
    */
  def initialise(destVaultInfo:UserInfo) = Try {
    new DirectCopier(MatrixStore.openVault(destVaultInfo))
  }
}

class DirectCopier(destVault:Vault) {
  private val logger = LoggerFactory.getLogger(getClass)
  val defaultChunkSize:Int = 5*(1024*1024)

  /**
    * calls the static Copier object to perform an individual file copy.
    * Abstracted to its own protected method here for easy mocking in tests
    * @return
    */
  protected def doCopyTo(vault:Vault, destFileName:Option[String], fromFile:File, chunkSize:Int, checksumType:String, keepOnFailure:Boolean=false,retryOnFailure:Boolean=true)(implicit ec:ExecutionContext,mat:Materializer):Future[(String,Option[String])] =
    Copier.doCopyTo(vault, destFileName, fromFile, chunkSize, checksumType, keepOnFailure, retryOnFailure)

  /**
    * performs the copy of all listed files in the [[ToCopy]] structure provided.
    * intended to be called from a mapAsync() stage in the stream.
    * @param from a [[ToCopy]] object indicating what should be copied
    * @return a Future contining an updated [[ToCopy]] object
    */
  def performCopy(from:ToCopy, copyChunkSize:Option[Int]=None)(implicit ec:ExecutionContext, actorSystem:ActorSystem, mat:Materializer) = {
    val itemsToCopy:Seq[Path] = Seq(
      Some(from.sourceFile.path),
      from.proxyMedia.map(_.path),
      from.thumbnail.map(_.path)
    ).collect({case Some(path)=>path})

    val mediaResult = Future.sequence(
      itemsToCopy.map(filePath=>
        doCopyTo(destVault,
          None,
          filePath.toFile,
          copyChunkSize.getOrElse(defaultChunkSize),
          "md5",
          retryOnFailure = false)
      )
    )

    mediaResult.map(results=>{
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

      from.copy(
        sourceFile = updatedSourceFile,
        proxyMedia = updatedProxyMedia,
        thumbnail = updatedThumbnail
      )
    })
  }
}

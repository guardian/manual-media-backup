package streamcomponents

import helpers.MatrixStoreHelper
import models.{CustomMXSMetadata, ToCopy}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object MetadataHelper {
  /**
    * fetches filesystem metadata
    * @param req [[ToCopy]] request describing the fileset to be copied
    * @return a new [[ToCopy]] object with filesystem metadata attached.  This is in a Future for calling with .mapAsync on a stream.
    */
  def gatherMetadata(req:ToCopy, maybeType:Option[String]=None):Future[ToCopy] = Future {
    val mediaMeta = MatrixStoreHelper.metadataFromFilesystem(req.sourceFile.toFile)
    Future
      .fromTry(mediaMeta)
      .map(_.withString("GNM_TYPE",maybeType.getOrElse(CustomMXSMetadata.TYPE_RUSHES)))
      .map(req.withMetadata)
  }.flatten
}

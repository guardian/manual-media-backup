package models

import akka.stream.Materializer
import com.om.mxs.client.japi.{MXFSFileAttributes, Vault}
import helpers.MetadataHelper

import scala.concurrent.ExecutionContext

case class ObjectMatrixEntry(oid:String, vault:Vault, attributes:Option[MxsMetadata], fileAttribues:Option[FileAttributes]) {
  def getMxsObject = vault.getObject(oid)

  def getMetadata(implicit mat:Materializer, ec:ExecutionContext) = MetadataHelper
    .getAttributeMetadata(getMxsObject)
    .map(mxsMeta=>
      this.copy(oid, vault, Some(mxsMeta), Some(FileAttributes(MetadataHelper.getMxfsMetadata(getMxsObject))))
    )

}
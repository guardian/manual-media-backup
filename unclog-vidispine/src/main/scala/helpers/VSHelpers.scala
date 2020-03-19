package helpers

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import akka.stream.Materializer
import models.S3Target
import vidispine.VSCommunicator.OperationType
import vidispine.{FieldNames, VSCommunicator, VSLazyItem}
import scala.concurrent.ExecutionContext.Implicits.global

object VSHelpers {
  def setArchivalMetadataFields(vsItemId:String, archivedTo:S3Target, archiveDate:Option[ZonedDateTime]=None)(implicit vsCommunicator:VSCommunicator, mat:Materializer) = {
    val vsDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXX")
    val vsDateOnly = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val commitTime = archiveDate.getOrElse(ZonedDateTime.now())
    val xmlDoc = <MetadataDocument xmlns="http://xml.vidispine.com/schema/vidispine">
      <group>Asset</group>
      <timespan start="-INF" end="+INF">
        <group>
          <name>ExternalArchiveRequest</name>
          <field>
            <name>{FieldNames.EXTERNAL_ARCHIVE_STATUS}</name>
            <value>Archived</value>
          </field>
          <field>
            <name>{FieldNames.EXTERNAL_ARCHIVE_COMMITTED_AT}</name>
            <value>{commitTime.format(vsDateFormat)}</value>
          </field>
          <field>
            <name>{FieldNames.EXTERNAL_ARCHIVE_DEVICE}</name>
            <value>{archivedTo.bucket}</value>
          </field>
          <field>
            <name>{FieldNames.EXTERNAL_ARCHIVE_PATH}</name>
            <value>{archivedTo.path}</value>
          </field>
          <field>
            <name>{FieldNames.EXTERNAL_ARCHIVE_REPORT}</name>
            <value>{commitTime.format(vsDateOnly)} Archived out by unclog-vidispine</value>
          </field>
        </group>
      </timespan>
    </MetadataDocument>

    val uri = s"/API/item/${vsItemId}/metadata"
    vsCommunicator.request(OperationType.PUT,uri,Some(xmlDoc.toString()),Map("Content-Type"->"application/xml"))
  }
}

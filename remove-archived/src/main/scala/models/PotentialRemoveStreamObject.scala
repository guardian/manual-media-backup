package models

case class PotentialRemoveStreamObject(omFile:ObjectMatrixEntry, status:Option[ArchiveStatus.Value], archivedSize:Option[Long])
object PotentialRemoveStreamObject {
  def apply(omFile: ObjectMatrixEntry, status: Option[ArchiveStatus.Value]): PotentialRemoveStreamObject = new PotentialRemoveStreamObject(omFile, status,None)
  def apply(omFile:ObjectMatrixEntry) = new PotentialRemoveStreamObject(omFile, None,None)
}

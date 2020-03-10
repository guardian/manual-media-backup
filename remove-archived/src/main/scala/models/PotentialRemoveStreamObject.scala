package models

case class PotentialRemoveStreamObject(omFile:ObjectMatrixEntry, status:Option[ArchiveStatus.Value])
object PotentialRemoveStreamObject {
  def apply(omFile: ObjectMatrixEntry, status: Option[ArchiveStatus.Value]): PotentialRemoveStreamObject = new PotentialRemoveStreamObject(omFile, status)
  def apply(omFile:ObjectMatrixEntry) = new PotentialRemoveStreamObject(omFile, None)
}

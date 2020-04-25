package models

case class BackupDebugInfo(filePath:String, notes:Option[String], potentialMatchSizes:Seq[Long])
case class EstimateCounter(needsBackupSize:Option[Long],noBackupSize:Option[Long],filePath:BackupDebugInfo)

case class FinalEstimate(needsBackupCount:Int, needsBackupSize:Long, noBackupCount:Int, noBackupSize:Long, pathsToBackUp:Seq[BackupDebugInfo])
object FinalEstimate {
  def apply(needsBackupCount: Int, needsBackupSize: Long, noBackupCount: Int, noBackupSize: Long): FinalEstimate =
    new FinalEstimate(needsBackupCount, needsBackupSize, noBackupCount, noBackupSize, Seq())

  def empty = new FinalEstimate(0,0,0,0, Seq())
}
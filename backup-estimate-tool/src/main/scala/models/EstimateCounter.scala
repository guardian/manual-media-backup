package models

case class EstimateCounter(needsBackupSize:Option[Long],noBackupSize:Option[Long],filePath:String)

case class FinalEstimate(needsBackupCount:Int, needsBackupSize:Long, noBackupCount:Int, noBackupSize:Long, pathsToBackUp:Seq[String])
object FinalEstimate {
  def apply(needsBackupCount: Int, needsBackupSize: Long, noBackupCount: Int, noBackupSize: Long): FinalEstimate =
    new FinalEstimate(needsBackupCount, needsBackupSize, noBackupCount, noBackupSize, Seq())

  def empty = new FinalEstimate(0,0,0,0, Seq())
}
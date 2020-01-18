package models

case class EstimateCounter(needsBackupSize:Option[Long],noBackupSize:Option[Long])

case class FinalEstimate(needsBackupCount:Int, needsBackupSize:Long, noBackupCount:Int, noBackupSize:Long)
object FinalEstimate {
  def apply(needsBackupCount: Int, needsBackupSize: Long, noBackupCount: Int, noBackupSize: Long): FinalEstimate =
    new FinalEstimate(needsBackupCount, needsBackupSize, noBackupCount, noBackupSize)

  def empty = new FinalEstimate(0,0,0,0)
}
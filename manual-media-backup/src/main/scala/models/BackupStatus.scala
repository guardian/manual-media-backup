package models

object BackupStatus extends Enumeration {
  val NOT_STARTED, NO_BACKUP_NEEDED,BACKED_UP,FAILED = Value
}

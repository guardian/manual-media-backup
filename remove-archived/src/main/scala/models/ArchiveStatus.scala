package models

object ArchiveStatus extends Enumeration {
  val SHOULD_KEEP, NOT_ARCHIVED, ARCHIVE_CONFLICT, SAFE_TO_DELETE = Value
}

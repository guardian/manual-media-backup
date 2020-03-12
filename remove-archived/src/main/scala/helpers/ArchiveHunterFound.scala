package helpers

trait ArchiveHunterLookupResult

case class ArchiveHunterFound(archiveHunterId:String, archiveHunterCollection:String, size:Option[Long], beenDeleted:Boolean) extends ArchiveHunterLookupResult
case object ArchiveHunterNotFound extends ArchiveHunterLookupResult
package archivehunter

trait ArchiveHunterLookupResult

case class ArchiveHunterFound(archiveHunterId:String, archiveHunterCollection:String, size:Long, beenDeleted:Boolean) extends ArchiveHunterLookupResult
case object ArchiveHunterNotFound extends ArchiveHunterLookupResult

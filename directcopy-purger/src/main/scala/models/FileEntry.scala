package models

import java.nio.file.attribute.PosixFileAttributeView
import java.nio.file.{Files, Path}
import scala.util.Try

case class LocalFile(filePath:Path, length: Long, md5:Option[String])

case class RemoteFile(oid:String, length:Long, md5:Option[String])

case class FileEntry(localFile:LocalFile, remoteFile:Option[RemoteFile])

object FileEntry {
  def fromPath(path:Path) = Try {
    FileEntry(
      LocalFile(path, Files.size(path), None),
      None
    )
  }
}
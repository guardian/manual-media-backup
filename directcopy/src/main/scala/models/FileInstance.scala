package models

import java.nio.file.Path

case class FileInstance(path:Path, oid:Option[String], omChecksum:Option[String]) {
  /**
    * adds in data from a successful copy
    * @param copyData 2-tuple consisiting of (oid, optional checksum)
    * @return updated FileInstance
    */
  def withCopyData(copyData:(String, Option[String])):FileInstance = this.copy(
    oid=if(copyData._1=="") None else Some(copyData._1),
    omChecksum=copyData._2
  )

  /**
    * get a java.io.File pointing to the local file
    * @return java.io.File taken by applying `toFile` to the `path` property
    */
  def toFile:java.io.File = path.toFile
}

object FileInstance extends ((Path, Option[String], Option[String])=>FileInstance) {
  def apply(from:Path) = new FileInstance(from, None, None)
}
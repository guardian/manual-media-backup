package models

import java.nio.file.Path

case class ToCopy(sourceFile:Path, proxyMedia:Option[Path], thumbnail:Option[Path])

object ToCopy extends ((Path, Option[Path], Option[Path])=>ToCopy) {
  def apply(sourceFile:Path) = new ToCopy(sourceFile, None, None)
}

package models

import java.nio.file.Path

case class ToCopy(sourceFile:Path, proxyMedia:Option[Path], thumbnail:Option[Path], commonMetadata:Option[MxsMetadata]) {
  def withMetadata(meta:MxsMetadata):ToCopy = this.copy(commonMetadata = Some(meta))
}

object ToCopy extends ((Path, Option[Path], Option[Path], Option[MxsMetadata])=>ToCopy) {
  def apply(sourceFile:Path) = new ToCopy(sourceFile, None, None, None)
}

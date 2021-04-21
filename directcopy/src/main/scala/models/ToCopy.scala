package models

import java.nio.file.Path

/**
  * represents a single media instance going through the copying stream
  * @param sourceFile [[FileInstance]] pointing to the source file
  * @param proxyMedia optional [[FileInstance]] pointing to the proxy media, if present
  * @param thumbnail optional [[FileInstance]] pointing to the thumbnail media, if present
  * @param commonMetadata optional MxsMetadata instance of common metadata to apply to all files
  */
case class ToCopy(sourceFile:FileInstance,
                  proxyMedia:Option[FileInstance],
                  thumbnail:Option[FileInstance],
                  commonMetadata:Option[MxsMetadata]) {
  def withMetadata(meta:MxsMetadata):ToCopy = this.copy(commonMetadata = Some(meta))
}

object ToCopy extends ((FileInstance, Option[FileInstance], Option[FileInstance], Option[MxsMetadata])=>ToCopy) {
  def apply(sourceFile:Path) = new ToCopy(FileInstance(sourceFile), None, None, None)
  def apply(sourceFile:Path, maybeProxy:Option[Path], maybeThumb:Option[Path]) =
    new ToCopy(FileInstance(sourceFile),
      maybeProxy.map(FileInstance.apply),
      maybeThumb.map(FileInstance.apply),
      None
    )
  def apply(sourceFile:Path, commonMetadata:Option[MxsMetadata]) =
    new ToCopy(FileInstance(sourceFile), None, None, commonMetadata)
}

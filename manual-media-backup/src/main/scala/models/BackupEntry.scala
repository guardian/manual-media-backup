package models

import java.nio.file.Path

case class BackupEntry(originalPath:Path, maybeObjectMatrixEntry:Option[ObjectMatrixEntry])

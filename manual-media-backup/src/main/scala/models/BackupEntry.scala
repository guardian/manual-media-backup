package models

import java.nio.file.Path

case class BackupEntry(originalPath:Path,
                       maybeObjectMatrixEntry:Option[ObjectMatrixEntry]=None,
                       status:BackupStatus.Value=BackupStatus.NOT_STARTED,
                       timelag:Option[Long]=None)

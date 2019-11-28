package models

/**
  * this is intentionally a subset of MediaCensus' CensusEntry, containing the VS-specific parts we need.
  * This is needed to drive our actions from a MediaCensus search, specifically the "needs backup" one.
  * @param sourceStorage
  * @param storageSubpath
  * @param vsFileId
  * @param vsItemId
  * @param vsShapeIds
  * @param replicas
  * @param replicaCount
  */
case class VSBackupEntry(sourceStorage:Option[String], storageSubpath:Option[String], vsFileId:Option[String], vsItemId:Option[String],
                         vsShapeIds:Option[Seq[String]], replicas:Seq[VSFileLocation], replicaCount:Int,
                         newlyCreatedReplicaId:Option[String], fullPath:Option[String], vidispineMD5:Option[String], vidispineSize:Option[Long])

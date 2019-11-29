package models

case class CustomMXSMetadata(itemType:String, projectId:Option[String], commissionId:Option[String], masterId:Option[String],
                             projectName:Option[String],commissionName:Option[String],workingGroupName:Option[String]) {
  /**
    * adds the contents of the record to the given MxsMetadata object, ignoring empty fields
    * @param addTo existing [[MxsMetadata]] object to add to; this can be `MxsMetadata.empty`
    * @return a new, updated [[MxsMetadata]] object
    */
  def toAttributes(addTo:MxsMetadata):MxsMetadata = {
    val content = Seq(
      projectId.map(s=>"GNM_PROJECT_ID"->s),
      commissionId.map(s=>"GNM_COMMISSION_ID"->s),
      masterId.map(s=>"GNM_MASTER_ID"->s),
      projectName.map(s=>"GNM_PROJECT_NAME"->s),
      commissionName.map(s=>"GNM_COMMISSION_NAME"->s),
      workingGroupName.map(s=>"GNM_WORKING_GROUP_NAME"->s)
    ).collect({case Some(kv)=>kv}).toMap

    content.foldLeft(addTo)((acc,kv)=>acc.withString(kv._1,kv._2))
  }
}

object CustomMXSMetadata {
  val TYPE_MASTER = "master"
  val TYPE_RUSHES = "rushes"
  val TYPE_DELIVERABLE = "deliverables"
  val TYPE_UNSORTED = "unsorted"

  def fromMxsMetadata(incoming:MxsMetadata):Option[CustomMXSMetadata] =
    incoming.stringValues.get("GNM_TYPE").map(itemType=>
      new CustomMXSMetadata(itemType, incoming.stringValues.get("GNM_PROJECT_ID"), incoming.stringValues.get("GNM_COMMISSION_ID"),
        incoming.stringValues.get("GNM_MASTER_ID"),incoming.stringValues.get("GNM_PROJECT_NAME"),incoming.stringValues.get("GNM_COMMISSION_NAME"),
        incoming.stringValues.get("GNM_WORKING_GROUP_NAME")
      )
    )
}
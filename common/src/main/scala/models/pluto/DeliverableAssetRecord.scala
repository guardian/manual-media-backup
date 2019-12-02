package models.pluto

import java.time.LocalDateTime

case class DeliverableAssetRecord(id:Int,
                                  `type`:Option[String],
                                  filename:String,
                                  size:Long,
                                  access_dt:LocalDateTime,
                                  modified_dt:LocalDateTime,
                                  changed_dt:LocalDateTime,
                                  job_id:Option[String],
                                  item_id:Option[String],
                                  deliverable:Int,
                                  has_ongoing_job:Option[Boolean],
                                  status:Option[Int],
                                  type_string:Option[String],
                                  version:Option[Int])

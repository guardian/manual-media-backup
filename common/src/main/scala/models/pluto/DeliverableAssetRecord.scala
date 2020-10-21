package models.pluto

import java.time.{LocalDateTime, ZonedDateTime}

case class DailyMotionMaster(
                            id:Int,
                            daily_motion_url: Option[String],
                            daily_motion_title: Option[String],
                            daily_motion_description: Option[String],
                            daily_motion_tags: Option[Seq[String]],
                            daily_motion_category: Option[String],
                            publication_date: Option[ZonedDateTime],
                            upload_status: Option[String],
                            daily_motion_no_mobile_access: Option[Boolean],
                            daily_motion_contains_adult_content: Option[Boolean],
                            etag: Option[String]
                            )

/*
fill in the extra fields here as they are needed
 */
case class MainstreamMaster(id:Int)

case class GNMMaster(id:Int)

case class YoutubeMaster(id:Int)

case class DeliverableAssetRecord(id:Int,
                                  `type`:Option[Int],
                                  filename:String,
                                  size:Long,
                                  access_dt:LocalDateTime,
                                  modified_dt:LocalDateTime,
                                  changed_dt:LocalDateTime,
                                  job_id:Option[String],
                                  online_item_id:Option[String],
                                  nearline_item_id:Option[String],
                                  archive_item_id:Option[String],
                                  deliverable:DeliverableBundleRecord,
                                  status:Option[Int],
                                  type_string:Option[String],
                                  atom_id:Option[String],
                                  gnm_website_master:Option[GNMMaster],
                                  youtube_master:Option[YoutubeMaster],
                                  DailyMotion_master:Option[DailyMotionMaster],
                                  mainstream_master:Option[MainstreamMaster])

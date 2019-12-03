package models.pluto

import java.time.LocalDateTime

case class MasterRecord (user:Option[Int],
                        title:String,
                        created:LocalDateTime,
                        updated:LocalDateTime,
                        duration:String,
                        commission:Int,
                        project:Int,
                        gnm_master_standfirst:Option[String],
                        gnm_master_website_headline:Option[String],
                        gnm_master_generic_status:Option[String],
                        gnm_master_generic_intendeduploadplatforms:Option[String],
                        gnm_master_generic_publish:Option[String],
                        gnm_master_generic_remove:Option[String]
                        )
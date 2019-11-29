package models.pluto

import java.time.LocalDateTime
import java.util.UUID

case class CommissionRecord(collection_id:Int, user:Int, created:LocalDateTime, updated:LocalDateTime,
                            gnm_commission_title:String, gnm_commission_status:String, gnm_commission_workinggroup:UUID,
                            gnm_commission_description:Option[String], gnm_commission_owner:List[Int])

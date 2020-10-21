package models.pluto

import java.sql.Timestamp
import java.time.LocalDateTime

case class CommissionRecord(id:Int, collectionId:Option[Int], siteId: Option[String], created: LocalDateTime, updated:LocalDateTime,
                            title: String, status: String, description: Option[String], workingGroup: Int,
                            originalCommissionerName:Option[String], scheduledCompletion:LocalDateTime, owner:String,
                            notes:Option[String],
                            productionOffice:String,
                            originalTitle:Option[String])

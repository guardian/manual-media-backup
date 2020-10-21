package models.pluto

import java.sql.Timestamp
import java.time.{LocalDateTime, ZonedDateTime}

case class CommissionRecord(id:Int, created: ZonedDateTime, updated:ZonedDateTime,
                            title: String, status: String, workingGroupId: Int,
                            scheduledCompletion:LocalDateTime, owner:String,
                            productionOffice:String)

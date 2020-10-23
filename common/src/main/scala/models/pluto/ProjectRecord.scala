package models.pluto

import java.time.{LocalDateTime, ZonedDateTime}


case class ProjectRecord (id: Int, projectTypeId: Int, vidispineId: Option[String],
                          title: String, created:ZonedDateTime, updated:ZonedDateTime, user: String, workingGroupId: Option[Int],
                          commissionId: Option[Int], deletable: Option[Boolean], deep_archive: Option[Boolean],
                          sensitive: Option[Boolean], status:String, productionOffice: String)

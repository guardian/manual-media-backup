package models.pluto

import java.time.LocalDateTime


case class ProjectRecord (id: Int, projectTypeId: Int, vidispineProjectId: Option[String],
                          projectTitle: String, created:LocalDateTime, updated:LocalDateTime, user: String, workingGroupId: Option[Int],
                          commissionId: Option[Int], deletable: Option[Boolean], deep_archive: Option[Boolean],
                          sensitive: Option[Boolean], status:String, productionOffice: String)

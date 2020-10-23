package models.pluto

import java.time.LocalDateTime

case class DeliverableBundleRecord (project_id:Option[String],commission_id:Option[Int], pluto_core_project_id: Int, name:Option[String],created:LocalDateTime)
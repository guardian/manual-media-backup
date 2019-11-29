package models.pluto

import java.time.LocalDateTime
import java.util.UUID

/*
{
  "collection_id": 13,
  "user": 1,
  "created": "2018-08-01T12:21:12.532",
  "updated": "2019-02-26T10:21:53.518",
  "commission": 10,
  "gnm_project_status": "Held",
  "gnm_project_standfirst": null,
  "gnm_project_headline": "dasdsadsadsa test 2",
  "gnm_project_username": [
    1
  ],
  "gnm_project_project_file_item": null,
  "gnm_project_prelude_file_item": null,
  "gnm_project_type": "0470ff3b-603d-456d-8ea3-e391ddbe11ce",
  "project_locker_id": null,
  "project_locker_id_prelude": null
}
 */
case class ProjectRecord (collection_id:Int, user:Int, created:LocalDateTime, updated:LocalDateTime, commission:Int, gnm_project_status:String,
                          gnm_project_standfirst:Option[String], gnm_project_headline:Option[String], gnm_project_username:List[Int],
                          gnm_project_file_item:Option[String], gnm_prelude_file_item:Option[String], gnm_project_type:UUID,
                          project_locker_id:Option[String], project_locked_id_prelude:Option[String])

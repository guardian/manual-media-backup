package models

import akka.http.scaladsl.model.ContentType

case class S3Target(bucket:String, path:String, maybeContentType:Option[ContentType])

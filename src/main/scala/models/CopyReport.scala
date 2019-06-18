package models

case class CopyReport (filename:String, oid:String, checksum:Option[String], size:Long, preExisting:Boolean, validationPassed:Option[Boolean])
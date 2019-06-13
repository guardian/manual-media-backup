package models

import com.om.mxs.client.japi.MxsObject
import helpers.MetadataHelper

case class MxsMetadata (stringValues:Map[String,String], boolValues:Map[String,Boolean], longValues:Map[String,Long], intValues:Map[String,Int])

object MxsMetadata {
  def apply(stringValues: Map[String, String], boolValues: Map[String, Boolean], longValues: Map[String, Long], intValues: Map[String, Int]): MxsMetadata = new MxsMetadata(stringValues, boolValues, longValues, intValues)

}

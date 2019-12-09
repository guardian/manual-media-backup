package streamComponents

import com.gu.vidispineakka.vidispine.{VSFile, VSLazyItem, VSShape}

trait FilenameHelpers {
  private val extensionExtractor = "^(.*)\\.([^\\.]+)$".r

  /**
    * try to get the file extension for the given file
    * @param forPath
    * @return
    */
  def getExtension(forPath:String) = forPath match {
    case extensionExtractor(_, xtn)=>Some(xtn)
    case _=>None
  }

  /**
    * remove the extension for the given file
    * @param forPath
    * @return
    */
  def removeExtension(forPath:String) = forPath match {
    case extensionExtractor(path, _)=>path
    case _=>forPath
  }

  /**
    * try to determine the correct file for an upload.
    * prefers the "gnm_asset_filename" field from the item, but if that can't be found then use the "path" field
    * from any valid file on the given shape
    * @param fromItem VSLazyItem to check direct metadata
    * @param fromShape VSShape to check indirect metadata
    * @return either the filepath or None if no path could be determined
    */
  def determineFileName(fromItem:VSLazyItem, fromShape:Option[VSShape]) = {
    fromItem.getSingle("gnm_asset_filename") match {
      case Some(fileName)=>
        Some(fileName)
      case None=>
        fromShape.flatMap(_.files.map(_.path).headOption)
    }
  }

  /**
    * applies the correct file extension (the one present on the proxy shape) to the determined file name (probably from the
    * original media)
    * @param determinedName filename as determined by determineFileName
    * @param actualFile an actual VSFile with the correct extension
    * @return an updated filename
    */
  def fixFileExtension(determinedName:String, actualFile:VSFile) = {
    val correctXtn = getExtension(actualFile.path)
    determinedName match {
      case extensionExtractor(fullPath, _)=>
        fullPath + correctXtn.map(x=>"." + x).getOrElse("")
      case _=>  //there was no extension
        determinedName + correctXtn.map(x=>"." + x).getOrElse("")
    }
  }
}

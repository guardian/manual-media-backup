package helpers

import java.io.{File, FileReader}
import java.util.Properties

class StoragePathMap(forFile:File) {
  private def loadFile = {
    val reader = new FileReader(forFile)
    try {
      val props = new Properties()
      props.load(reader)
      props
    } finally {
      reader.close()
    }
  }

  private val storagePathProps = loadFile

  /**
    * if a path prefix is defined for the given storage then return it, otherwise return none
    * @param storageId storage ID to check
    * @return an Option containing the defined path prefix if present
    */
  def pathPrefixForStorage(storageId:String):Option[String] = Option(storagePathProps.getProperty(storageId))

}

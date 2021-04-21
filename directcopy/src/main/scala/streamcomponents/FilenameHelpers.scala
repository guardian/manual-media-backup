package streamcomponents

import java.nio.file.Path

object FilenameHelpers {
  private val xtractor = "(.*)\\.([^.]+)$".r
  case class PathXtn(path:String, xtn:Option[String]) {
    def hasExtension:Boolean = xtn.isDefined
  }

  /**
    * extracts the filename portion of a file and splits it into path and extension pieces
    */
  object PathXtn {
    def apply(forPath:Path) = {
      forPath.toString match {
        case FilenameHelpers.xtractor(path,xtn)=>new PathXtn(path, Some(xtn))
        case path:String=>new PathXtn(path, None)
      }
    }
  }
}

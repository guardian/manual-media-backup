package models

import java.nio.file.{Path, Paths}
import scala.util.{Failure, Try}

case class PathTransform(from:Path, to:Path) {
  /**
    * returns a boolean indicating whether this transform is applicable to the given file, i.e. whether they share a common base.
    * If this returns false, then running `apply` against the path will result in a Failure
    * @param mediaFile media file you might want to apply to
    * @return True if it will work otherwise False.
    */
  def canApplyTo(mediaFile:Path):Boolean = {
    mediaFile.startsWith(from)
  }

  /**
    * applies this path transform to a given incoming path. This operation can fail if it's not possible to relativise
    * the `mediaFile` path to the `from` path.
    * @param mediaFile
    * @return
    */
  def apply(mediaFile:Path):Try[Path] = {
    if(!canApplyTo(mediaFile)) return Failure(new RuntimeException(s"Media file $mediaFile does not belong to transformation base path $from"))
    Try {
      val basePath = from.relativize(mediaFile)
      to.resolve(basePath)
    }
  }
}

object PathTransform extends ((Path, Path)=>PathTransform){
  def apply(from:String, to:String) = {
    new PathTransform(
      Paths.get(from),
      Paths.get(to)
    )
  }

  private val specSplitter = "^(.*)=(.*)$".r

  def fromPathSpec(spec:String) = {
    spec match {
      case specSplitter(from,to) => Right(apply(from,to))
      case _=> Left("The given path spec could not be interpreted")
    }
  }
}
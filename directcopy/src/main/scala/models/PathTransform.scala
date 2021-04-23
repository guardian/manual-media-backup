package models

import java.nio.file.{Path, Paths}
import scala.util.{Failure, Success, Try}
import cats.implicits._

case class PathTransform(from:Path, to:Path, stripComponents:Option[Int]) {
  /**
    * returns a boolean indicating whether this transform is applicable to the given file, i.e. whether they share a common base.
    * If this returns false, then running `apply` against the path will result in a Failure
    * @param mediaFile media file you might want to apply to
    * @return True if it will work otherwise False.
    */
  def canApplyTo(mediaFile:Path):Boolean = {
    mediaFile.startsWith(from)
  }

  def withoutStripComponents = stripComponents match {
    case Some(_)=>this.copy(stripComponents=None)
    case None=>this
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
      val finalPath = to.resolve(basePath)
      stripComponents match {
        case Some(componentCount)=>
          finalPath.subpath(componentCount,finalPath.getNameCount)
        case None=>
          finalPath
      }
    }
  }
}

object PathTransform extends ((Path, Path, Option[Int])=>PathTransform){
  def apply(from:String, to:String) = {
    new PathTransform(
      Paths.get(from),
      Paths.get(to),
      None
    )
  }
  def apply(from:String, to:String, pathStrip:Option[Int]) = {
    new PathTransform(
      Paths.get(from),
      Paths.get(to),
      pathStrip
    )
  }

  private val specSplitter = "^(.*)=(.*)$".r

  def fromPathSpec(spec:String, pathStrip:Option[Int]=None) = {
    spec match {
      case specSplitter(from,to) => Right(apply(from,to, pathStrip))
      case _=> Left("The given path spec could not be interpreted")
    }
  }
}

class PathTransformSet(transforms:Seq[PathTransform]) {
  /**
    * if any pathTransforms are set, then find one that will apply to the incoming path and use it
    * @param filePath media file path to apply the change to
    * @return a Future, containing an Option with the transformed path if a transformer is set or None if not.
    */
  protected def maybeTransformFilepath(filePath:Path, currentTransform:Option[PathTransform], remainingTransforms:Seq[PathTransform]):Try[Option[Path]] = {
    if(currentTransform.isDefined && currentTransform.get.canApplyTo(filePath)) {
      currentTransform
        .map(_.apply(filePath))
        .sequence //courtesy of cats, changes Option[Try[A]] to Try[Option[A]]
    } else {
      if(remainingTransforms.nonEmpty) {
        maybeTransformFilepath(filePath, remainingTransforms.headOption, remainingTransforms.tail)
      } else {
        Success(None)
      }
    }
  }

  def apply(filePath:Path):Try[Option[Path]] = {
    val listTail = if(transforms.nonEmpty) {
      transforms.tail
    } else {
      Seq()
    }
    maybeTransformFilepath(filePath, transforms.headOption, listTail)
  }

  def withoutStripComponents = new PathTransformSet(transforms.map(_.withoutStripComponents))
}

object PathTransformSet {
  def empty = new PathTransformSet(Seq())
}
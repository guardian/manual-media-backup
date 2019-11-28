package streamcomponents

import java.nio.file.{Files, Path}

import akka.NotUsed
import akka.stream.scaladsl.Source
import scala.collection.JavaConverters._

object FileListSource {
  def apply(startingAt: Path): Source[Path,NotUsed] = Source.fromIterator(()=>Files.walk(startingAt).iterator().asScala)
}

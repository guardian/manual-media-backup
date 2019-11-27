package streamcomponents

import java.nio.file.{Files, Path}

import akka.NotUsed
import akka.stream.scaladsl.Source

object FileListSource {
  def apply(startingAt: Path): Source[Path,NotUsed] = Source.fromIterator(()=>Files.walk(startingAt).iterator().asScala)
}

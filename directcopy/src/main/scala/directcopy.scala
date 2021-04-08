import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, SourceShape}
import akka.stream.scaladsl.{GraphDSL, Sink}
import streamcomponents.{FileListSource, FilterOutDirectories, FilterOutMacStuff, LocateProxyFlow}
import akka.stream.scaladsl.GraphDSL.Implicits._

import java.nio.file.Path
import scala.reflect.io.File

object directcopy {
  lazy implicit val actorSystem = ActorSystem("direct-copy")
  lazy implicit val mat = ActorMaterializer()

  /**
    * partial akka stream that lists out all of the source files we are interested in
    * @param startingPath
    * @return
    */
  def inputStream(startingPath:Path) =
    GraphDSL.create() { implicit builder=>
      val src = builder.add(FileListSource(startingPath))
      val dirFilter = builder.add(new FilterOutDirectories)
      val macStuffFilter = builder.add(new FilterOutMacStuff)

      src ~> dirFilter ~> macStuffFilter
      SourceShape(macStuffFilter.out)
    }

  def buildStream(startingPath:Path) = {
    val sinkFac = Sink.ignore
    GraphDSL.create(sinkFac) { implicit builder=>
      val src = inputStream(startingPath)
      val proxyLocator = builder.add(new LocateProxyFlow(sourceMediaPath, proxyMediaPath, proxyMediaPostfix, proxyMediaXtn, thumbnailPostfix, thumbnailXtn))

      src ~> proxyLocator
      ClosedShape
    }
  }
  def main(args:Array[String]) = {

  }
}

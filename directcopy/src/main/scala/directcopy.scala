import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, SourceShape}
import akka.stream.scaladsl.{GraphDSL, Sink}
import streamcomponents.{FileListSource, FilterOutDirectories, FilterOutMacStuff, LocateProxyFlow, UTF8PathCharset}
import akka.stream.scaladsl.GraphDSL.Implicits._
import helpers.Copier
import models.ToCopy

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.io.File

object directcopy {
  lazy implicit val actorSystem = ActorSystem("direct-copy")
  lazy implicit val mat = ActorMaterializer()
  lazy implicit val ec:ExecutionContext = actorSystem.dispatcher

  lazy val copyChunkSize = sys.env.get("CHUNK_SIZE").map(_.toInt).getOrElse(5*(1024*1024))

  /**
    * partial akka stream that lists out all of the source files we are interested in
    * @param startingPath
    * @return
    */
  def inputStream(startingPath:Path) =
    GraphDSL.create() { implicit builder=>
      val src = builder.add(FileListSource(startingPath))
      val pathFixer = builder.add(new UTF8PathCharset)
      val dirFilter = builder.add(new FilterOutDirectories)
      val macStuffFilter = builder.add(new FilterOutMacStuff)

      src ~> pathFixer ~> dirFilter ~> macStuffFilter
      SourceShape(macStuffFilter.out)
    }

  def performCopy(from:ToCopy) = {
   val itemsToCopy = Seq(
     Some(from.sourceFile),
     from.proxyMedia,
     from.thumbnail
   ).collect({case Some(path)=>path})

    val mediaResult = Future.sequence(
      itemsToCopy.map(filePath=>
        Copier.doCopyTo(destVault, None, filePath.toFile, copyChunkSize, "md5", keepOnFailure = false, retryOnFailure = false)
      )
    )

    mediaResult.map(results=>{
      val sourceMediaResult = results.head
      val proxyMediaResult =
    })
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

package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.{ToCopy, FileInstance, MxsMetadata}
import org.slf4j.LoggerFactory
import java.nio.file.Path
import scala.util.{Failure, Success, Try}
import cats.implicits._

/**
  * Akka flow component that attempts to locate a similarly-named proxy and thumbnail for a given media file.
  * It's expected that source media and thumbnail/proxy are in parallel directory structures, you must specify
  * the root of the source media and proxy media directories.  In order to match, a proxy must have the same relative
  * path as its source media.  Thumbnails must be in the same directory as the proxy media.
  * @param sourceMediaPath
  * @param proxyMediaPath
  * @param proxyMediaPostfix
  * @param proxyMediaXtn
  * @param thumbnailPostfix
  * @param thumbnailXtn
  */
class LocateProxyFlow(sourceMediaPath:Path, proxyMediaPath:Path,
                      proxyMediaPostfix:Option[String], proxyMediaXtnList:List[String],
                      thumbnailPostfix:Option[String], thumbnailXtn:String) extends GraphStage[FlowShape[Path, ToCopy]] {
  import FilenameHelpers._

  private val logger = LoggerFactory.getLogger(getClass)
  private final val in:Inlet[Path] = Inlet.create("LocateProxyFlow.in")
  private final val out:Outlet[ToCopy] = Outlet.create("LocateProxyFlow.out")

  override def shape: FlowShape[Path, ToCopy] = FlowShape.of(in, out)

  /**
    * checks whether the given Path exists. Included as a seperate method both for Scala composition
    * and for stubbing during testing
    * @param p path to check
    * @return a Boolean wrapped in a Try
    */
  protected def doesPathExist(p:Path) = Try {
    p.toFile.exists()
  }

  /**
    * try to find a proxy in the path provided at construction
    * @param directoryPath relative directory of the source media
    * @param fileXtn PathXtn instance containing the base filename and extension of the source media
    * @return either a Path pointing to the proxy or None
    */
  protected def findDependent(directoryPath:Path, optionalPostfix:Option[String], targetXtn:String, fileXtn:PathXtn):Try[Option[Path]] = {
    val targetFilename = optionalPostfix match {
      case Some(postfix)=>s"${fileXtn.path}$postfix.$targetXtn"
      case None=>s"${fileXtn.path}.$targetXtn"
    }
    val targetPath = directoryPath.resolve(targetFilename)
    logger.debug(s"Proxy search path is $targetPath")

    doesPathExist(targetPath).map(exists=>{
      if(exists) Some(targetPath) else None
    })
  }

  def findProxy(directoryPath:Path, fileXtn:PathXtn):Try[Option[Path]] = proxyMediaXtnList
    .map(findDependent(directoryPath, proxyMediaPostfix, _, fileXtn))
    .sequence
    .map(_.collect({ case Some(proxy)=>proxy}))
    .map(_.headOption)
     //.sequence is courtesy of cats - https://stackoverflow.com/questions/58074846/how-to-transform-a-seqtry-to-a-tryseq

  def findThumb(directoryPath:Path, fileXtn:PathXtn):Try[Option[Path]] = findDependent(directoryPath, thumbnailPostfix, thumbnailXtn, fileXtn)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val mediaPath = grab(in)

        val relativePath = sourceMediaPath.relativize(mediaPath)
        logger.debug(s"Relative base path for $sourceMediaPath is $relativePath")
        val pathXtn = PathXtn(relativePath)

        val searchResults = Seq(
          findProxy(proxyMediaPath, pathXtn),
          findThumb(proxyMediaPath, pathXtn)
        )

        val failures = searchResults.collect({case Failure(err)=>err})
        if(failures.nonEmpty) {
          logger.error(s"Could not check thumb and/or proxy for $mediaPath; ${failures.length} errors occurred. ${failures.map(_.getMessage).mkString(";")}")
          failStage(new RuntimeException("Could not check thumb and/or proxy, see logs for details."))
        } else {
          val successes = searchResults.collect({case Success(maybePath)=>maybePath})
          val result = ToCopy(FileInstance(mediaPath),
            successes.head.map(FileInstance.apply),
            successes(1).map(FileInstance.apply),
            Some(MxsMetadata.empty.withString("GNM_TYPE","Rushes"))
            )

          push(out, result)
        }
      }
    })
  }
}

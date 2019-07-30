import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import models.{VSBackupEntry, VSConfig}
import org.slf4j.LoggerFactory
import vidispine.VSCommunicator
import vsStreamComponents.{CreateFileDuplicate, DecodeMediaCensusOutput, LookupFullPath}
import com.softwaremill.sttp._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val actorSystem = ActorSystem("vs-media-backup")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  /**
    * asynchronously shuts down the actorsystem and then terminates the JVM session with the given exitcode
    * @param exitCode exitcode to return to system
    * @return a Future, which should effectively never resolve (JVM should quit as it does)
    */
  def terminate(exitCode:Int) = actorSystem.terminate().andThen({
    case _=>System.exit(exitCode)
  })

  lazy val vsConfig = VSConfig(
    uri"${sys.env("VIDISPINE_BASE_URL")}",
    sys.env("VIDISPINE_USER"),
    sys.env("VIDISPINE_PASSWORD")
  )

  lazy implicit val vsCommunicator = new VSCommunicator(vsConfig.vsUri, vsConfig.plutoUser, vsConfig.plutoPass)

  lazy val storageId:String = sys.env.get("DEST_STORAGE_ID") match {
    case Some(str)=>str
    case None=>
      logger.error("You must specify DEST_STORAGE_ID in the environment")
      Await.ready(terminate(1), 1 hour)
      ""  //we should never reach this point
  }

  def firstTestStream(dataSource:String) = {
    val sinkFactory = Sink.fold[Seq[VSBackupEntry],VSBackupEntry](Seq())((acc,elem)=>acc++Seq(elem))
    GraphDSL.create(sinkFactory) {implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new DecodeMediaCensusOutput(dataSource))
      val pathLookup = builder.add(new LookupFullPath(vsCommunicator))
      val duper = builder.add(new CreateFileDuplicate(vsCommunicator, storageId, dryRun = true))
      src ~> pathLookup ~> duper ~> sink
      ClosedShape
    }
  }

  def readResourceFile(name:String) = {
    val inputStream = getClass.getResourceAsStream("testdata.json")

    val src = scala.io.Source.fromInputStream(inputStream)
    val content = src.mkString
    inputStream.close()
    content
  }

  def main(args:Array[String]):Unit = {

    val testContent = readResourceFile("testdata.json")
    val graph = firstTestStream(testContent)

    RunnableGraph.fromGraph(graph).run().onComplete({
      case Failure(err)=>
        logger.error(s"Stream failed: ", err)
        terminate(1)
      case Success(results)=>
        logger.info("Stream completed.")
        results.foreach(entry=>logger.info(s"\tOutput: $entry"))
        terminate(0)
    })
  }
}

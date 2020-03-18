import java.nio.charset.Charset
import java.nio.file.Path

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Framing, GraphDSL, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.util.ByteString
import archivehunter.ArchiveHunterLookup
import models.PotentialArchiveTarget
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val actorSystem:ActorSystem = ActorSystem.create("unclog-vidispine")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  lazy val ahBaseUri = sys.env("ARCHIVE_HUNTER_URL")
  lazy val ahSecret = sys.env("ARCHIVE_HUNTER_SECRET")

  def buildStream(sourceFile:Path) = {
    val sinkFact = Sink.seq[PotentialArchiveTarget]

    GraphDSL.create(sinkFact) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val ahLookup = builder.add(new ArchiveHunterLookup(ahBaseUri, ahSecret))

      val src = FileIO
        .fromPath(sourceFile)
        .via(Framing.delimiter(ByteString("\n"),10240000,allowTruncation=false))
        .map(recordBytes=> PotentialArchiveTarget.fromMediaCensusJson(recordBytes.decodeString("UTF-8")))
          .map({
            case Success(target)=>target
            case Failure(err)=>
              logger.error(s"Could not decode incoming target: ", err)
              throw err
          })


      src ~> ahLookup
      ClosedShape
    }
  }
}

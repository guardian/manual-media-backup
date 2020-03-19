package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.util.ByteString
import models.PotentialArchiveTarget
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
  * parses a ByteString consisting of a single json object of data from MediaCensus into a PotentialArchiveTarget
  * optionally ignores and skips unreadable records
  * @param skipUnreadable if true (the default), skip over unreadable records
  */
class FromMediaCensusJson(skipUnreadable:Boolean=true) extends GraphStage[FlowShape[ByteString,PotentialArchiveTarget]] {
  private final val in:Inlet[ByteString] = Inlet.create("FromMediaCensusJson.in")
  private final val out:Outlet[PotentialArchiveTarget] = Outlet.create("FromMediaCensisJson.out")

  override def shape: FlowShape[ByteString, PotentialArchiveTarget] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger= LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        PotentialArchiveTarget.fromMediaCensusJson(elem.decodeString("UTF-8")) match {
          case Success(record)=>
            push(out, record)
          case Failure(err)=>
            if(skipUnreadable) {
              logger.warn(s"offending data was ${elem.decodeString("UTF-8")}")
              logger.warn("could not decode record from stream, skipping")
              pull(in)
            } else {
              logger.warn(s"offending data was ${elem.decodeString("UTF-8")}")
              failStage(new RuntimeException("could not decode record from stream, see logs for details"))
            }
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }

}

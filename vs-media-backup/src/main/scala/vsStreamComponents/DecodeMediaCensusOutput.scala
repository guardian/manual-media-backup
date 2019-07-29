package vsStreamComponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet, SourceShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.util.ByteString
import models.VSBackupEntry
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.auto._

import org.slf4j.LoggerFactory

class DecodeMediaCensusOutput (resultsPage:String) extends GraphStage[SourceShape[VSBackupEntry]]{
  private final val out:Outlet[VSBackupEntry] = Outlet.create("DecodeMediaCensusOutput.out")

  override def shape: SourceShape[VSBackupEntry] = SourceShape.of(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)
    private var queue:Seq[VSBackupEntry] = Seq()

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = {
        if(queue.nonEmpty){
          val nextEntry = queue.head
          queue = queue.tail
          push(out, nextEntry)
        } else {
          complete(out)
        }
      }
    })

    override def preStart(): Unit = {
      parse(resultsPage) match {
        case Left(parsingFailure)=>
          logger.error(s"Could not parse incoming json: $parsingFailure")
          failStage(new RuntimeException("Could not parse incoming json"))
        case Right(json)=>
          //the double-backslash operator returns a list of all matching keys, of which we have one... this must be cast into an array
          //which can then be mapped over to decode the individual objects.
          val decodeResults = (json \\ "entries").flatMap(_.asArray.getOrElse(Vector())).map(_.as[VSBackupEntry])
          println(decodeResults)
          val decodeFailures = decodeResults.collect({case Left(err)=>err})
          if(decodeFailures.nonEmpty){
            logger.error(s"Got ${decodeFailures.length} decode failures from ${decodeResults.length} results")
            decodeFailures.foreach(err=>logger.error(s"\t$err"))
            failStage(new RuntimeException("Could not decode incoming data"))
          }
          queue = queue ++ decodeResults.collect({case Right(result)=>result})
          logger.info(s"Got ${queue.length} new entries to run")
      }
    }
  }
}

package streamcomponents

import akka.actor.ActorRef
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic}
import models.{BackupEstimateEntry, BackupEstimateGroup}
import org.slf4j.LoggerFactory

/**
  * Basic sink that adds all incoming entries to the provided BackupEstimateGroup
  * @param toGroup ActorRef pointing to an instance of BackupEstimateGroup
  */
class BackupEstimateGroupSink(toGroup:ActorRef) extends GraphStage[SinkShape[BackupEstimateEntry]]{
  private final val in:Inlet[BackupEstimateEntry] = Inlet.create("BackupEstimateGroupSink.in")

  override def shape: SinkShape[BackupEstimateEntry] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)
        toGroup ! BackupEstimateGroup.AddToGroup(elem)
      }
    })

    override def preStart(): Unit = pull(in)
  }
}

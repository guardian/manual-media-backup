package streamcomponents

import java.nio.file.Path

import akka.actor.ActorRef
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic}
import models.{BackupEntry, CustomMXSMetadata}
import org.slf4j.LoggerFactory
import akka.pattern.ask

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class GatherMetadata (plutoCommunicator:ActorRef) extends GraphStage[FlowShape[BackupEntry, BackupEntry]] {
  import helpers.PlutoCommunicator._

  implicit val timeout:akka.util.Timeout = 60 seconds
  private final val in:Inlet[BackupEntry] = Inlet.create("GatherMetadata.in")
  private final val out:Outlet[BackupEntry] = Outlet.create("GatherMetadata.out")

  override def shape: FlowShape[BackupEntry, BackupEntry] = FlowShape.of(in,out)

  /**
    * looks up asset folder, project, commission and working group from the provided cache
    * @param basePath path to use for initial asset folder lookup
    * @param existingMxsMeta existing metadata for item
    * @return a Future containing an updated version of `existingMxsMeta`
    */
  def lookupAllMetaForRushes(basePath:Path, existingMxsMeta:CustomMXSMetadata) = {
    val assetFolderFut = (plutoCommunicator ? Lookup(basePath) ).mapTo[AFHMsg]
    assetFolderFut.flatMap({
      case LookupFailed=>Future.failed(new RuntimeException("Lookup failed, see previous logs"))
      case FoundAssetFolder(Some(assetFolder))=>
        val projectFut = (plutoCommunicator ? LookupProject(assetFolder.project)).mapTo[AFHMsg]
        projectFut.flatMap({
          case LookupFailed=>Future.failed(new RuntimeException("Lookup failed, see previous logs"))
          case FoundProject(Some(project))=>
            //fixme: this is a cheat but will work for now
            val commissionFut = (plutoCommunicator ? LookupCommission(s"VX-${project.commission}")).mapTo[AFHMsg]
            commissionFut.flatMap({
              case LookupFailed=>Future.failed(new RuntimeException("Lookup failed, see previous logs"))
              case FoundCommission(Some(commission))=>
                val workingGroupFut = (plutoCommunicator ? LookupWorkingGroup(commission.gnm_commission_workinggroup)).mapTo[AFHMsg]
                workingGroupFut.flatMap({
                  case LookupFailed=>Future.failed(new RuntimeException("Lookup failed, see previous logs"))
                  case FoundWorkingGroup(Some(workingGroup))=>
                    Future(existingMxsMeta.copy(
                      projectId=Some(assetFolder.project),
                      commissionId=Some(commission.collection_id.toString),
                      projectName=project.gnm_project_headline,
                      commissionName=Some(commission.gnm_commission_title),
                      workingGroupName=Some(workingGroup.name))
                    )
                  case FoundWorkingGroup(None)=>
                    Future(existingMxsMeta.copy(
                      projectId=Some(assetFolder.project),
                      commissionId=Some(commission.collection_id.toString),
                      projectName=project.gnm_project_headline,
                      commissionName=Some(commission.gnm_commission_title)
                    ))
                })
              case FoundCommission(None)=>
                Future(existingMxsMeta.copy(projectId=Some(assetFolder.project), projectName = project.gnm_project_headline))
            })
          case FoundProject(None)=>
            Future(existingMxsMeta.copy(projectId = Some(assetFolder.project)))
        })
      case FoundAssetFolder(None)=>
        Future(existingMxsMeta)
    })
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    val completedCb = createAsyncCallback[BackupEntry](e=>push(out,e))
    val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val maybeMetadata = elem.maybeObjectMatrixEntry.flatMap(_.attributes).flatMap(CustomMXSMetadata.fromMxsMetadata)
        val basePath = elem.originalPath.getParent

        logger.info(s"basePath is $basePath")

        val updatedCustomMeta = maybeMetadata match {
          case Some(existingMeta)=>
            existingMeta.itemType match {
              case CustomMXSMetadata.TYPE_RUSHES=>
                lookupAllMetaForRushes(basePath, existingMeta)
            }
        }

      }
    })
  }
}

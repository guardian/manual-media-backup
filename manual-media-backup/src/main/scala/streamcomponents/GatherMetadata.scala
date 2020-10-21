package streamcomponents

import java.nio.file.Path

import akka.actor.ActorRef
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.{BackupEntry, CustomMXSMetadata, MxsMetadata}
import org.slf4j.{Logger, LoggerFactory}
import akka.pattern.ask

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class GatherMetadata (plutoCommunicator:ActorRef) extends GraphStage[FlowShape[BackupEntry, BackupEntry]] {
  import helpers.PlutoCommunicator._
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val timeout:akka.util.Timeout = 240 seconds
  private final val in:Inlet[BackupEntry] = Inlet.create("GatherMetadata.in")
  private final val out:Outlet[BackupEntry] = Outlet.create("GatherMetadata.out")

  override def shape: FlowShape[BackupEntry, BackupEntry] = FlowShape.of(in,out)

  /**
    * look up project and associated commission and working group data
    * @param forProjectId vidispine project ID to look up
    * @param existingMxsMeta CustomMXSMetadata object representing the current state of file metadata
    * @return a Future containing a copy of `existingMxsMeta` that has been updated with known fields
    */
  def lookupProjectAndCommission(forProjectId:Int, existingMxsMeta:CustomMXSMetadata) = {
    val projectFut = (plutoCommunicator ? LookupProject(forProjectId)).mapTo[AFHMsg]
    projectFut.flatMap({
      case LookupFailed => Future.failed(new RuntimeException("Lookup failed, see previous logs"))
      case FoundProject(Some(project)) =>
        val commissionFut = (plutoCommunicator ? LookupCommission(project.commissionId)).mapTo[AFHMsg]
        commissionFut.flatMap({
          case LookupFailed => Future.failed(new RuntimeException("Lookup failed, see previous logs"))
          case FoundCommission(Some(commission)) =>
            val workingGroupFut = (plutoCommunicator ? LookupWorkingGroup(commission.workingGroupId)).mapTo[AFHMsg]
            workingGroupFut.flatMap({
              case LookupFailed => Future.failed(new RuntimeException("Lookup failed, see previous logs"))
              case FoundWorkingGroup(Some(workingGroup)) =>
                Future(existingMxsMeta.copy(
                  projectId = Some(forProjectId.toString),
                  commissionId = Some(commission.id.toString),
                  projectName = Some(project.title),
                  commissionName = Some(commission.title),
                  workingGroupName = Some(workingGroup.name))
                )
              case FoundWorkingGroup(None) =>
                Future(existingMxsMeta.copy(
                  projectId = Some(forProjectId.toString),
                  commissionId = Some(commission.id.toString),
                  projectName = Some(project.title),
                  commissionName = Some(commission.title)
                ))
            })
          case FoundCommission(None) =>
            Future(existingMxsMeta.copy(projectId = Some(forProjectId.toString), projectName = Some(project.title)))
        })
      case FoundProject(None) =>
        logger.warn(s"No project found for ID $forProjectId (got from asset folder db)")
        Future(existingMxsMeta.copy(projectId = Some(forProjectId.toString)))
    })
  }

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
        logger.info(s"Got asset folder ${assetFolder.path} for ${assetFolder.project} for $basePath")
        lookupProjectAndCommission(assetFolder.project, existingMxsMeta)
      case FoundAssetFolder(None)=>
        logger.info(s"Got no asset folder for $basePath")
        Future(existingMxsMeta)
    })
  }

  def lookupAllMetadataForDeliverables(deliverablePath:Path, existingMxsMeta:CustomMXSMetadata)(implicit logger:Logger) = {
    val assetFut = (plutoCommunicator ? LookupDeliverableAsset(deliverablePath.getFileName.toString)).mapTo[AFHMsg]

    assetFut.flatMap({
      case LookupFailed=>Future.failed(new RuntimeException("Lookup failed, see previous logs"))
      case FoundDeliverableAsset(Some(record))=>
            lookupProjectAndCommission(record.deliverable.pluto_core_project_id, existingMxsMeta).map(updatedMeta=>
              updatedMeta.copy(
                deliverableBundle = Some(record.deliverable.pluto_core_project_id),
                deliverableType = record.type_string,
                deliverableAssetId = Some(record.id)
              )
            )
      case FoundDeliverableAsset(None)=>
        logger.warn(s"Got no deliverable asset for path ${deliverablePath.toString}")
        Future(existingMxsMeta) //we did not find any record so we can't update anything
    })
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private implicit val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    val completedCb = createAsyncCallback[BackupEntry](e=>push(out,e))
    val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

    private var canComplete = true
    private var upstreamCompleted = false

    setHandler(in, new AbstractInHandler {
      override def onUpstreamFinish(): Unit = {
        if(canComplete) completeStage() else {
          logger.warn("Upstage completed but we are not ready yet")
          upstreamCompleted = true
        }
      }

      override def onPush(): Unit = {
        val elem = grab(in)

        val maybeMetadata = elem.maybeObjectMatrixEntry.flatMap(_.attributes).flatMap(CustomMXSMetadata.fromMxsMetadata)
        val basePath = elem.originalPath.getParent

        logger.info(s"basePath is $basePath")

        canComplete = false
        val updatedCustomMetaFut = maybeMetadata match {
          case Some(existingMeta)=>
            val currentMeta = if(elem.originalPath.getFileName.toString.startsWith(".")){
              logger.debug(s"${elem.originalPath.getFileName.toString} is a dot-file")
              existingMeta.copy(hidden = true)
            } else {
              existingMeta
            }

            currentMeta.itemType match {
              case CustomMXSMetadata.TYPE_RUSHES=>
                lookupAllMetaForRushes(basePath, currentMeta)
              case CustomMXSMetadata.TYPE_DELIVERABLE=>
                lookupAllMetadataForDeliverables(elem.originalPath, currentMeta)
              case _=>
                Future(currentMeta)
            }
          case None=>
            Future.failed(new RuntimeException("Incoming item had no type field so cannot determine metadata"))
        }

        updatedCustomMetaFut.onComplete({
          case Success(newmeta)=>
            val omEntry = elem.maybeObjectMatrixEntry.get
            val updatedEntry = omEntry.copy(attributes = Some(newmeta.toAttributes(omEntry.attributes.getOrElse(MxsMetadata.empty()))))
            val updatedBackupEntry = elem.copy(maybeObjectMatrixEntry = Some(updatedEntry))

            if(logger.isDebugEnabled) {
              updatedEntry.attributes match {
                case None =>
                  logger.debug(s"No new attributes for ${updatedEntry.oid}")
                case Some(attrs) =>
                  attrs.toAttributes().foreach(attr => logger.debug(s"\tGathered attributes ${attr.getKey} = ${attr.getValue} (${attr.getValue.getClass.toGenericString})"))
              }
            }
            completedCb.invoke(updatedBackupEntry)
            canComplete = true
            if(upstreamCompleted) completeStage()
          case Failure(err)=>
            logger.error(s"Could not look up metadata for ${elem.originalPath}: ", err)
            failedCb.invoke(err)
            canComplete = true
            if(upstreamCompleted) completeStage()
        })
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

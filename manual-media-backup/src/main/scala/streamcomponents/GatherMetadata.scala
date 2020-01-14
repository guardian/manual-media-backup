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

  implicit val timeout:akka.util.Timeout = 60 seconds
  private final val in:Inlet[BackupEntry] = Inlet.create("GatherMetadata.in")
  private final val out:Outlet[BackupEntry] = Outlet.create("GatherMetadata.out")

  override def shape: FlowShape[BackupEntry, BackupEntry] = FlowShape.of(in,out)

  /**
    * look up project and associated commission and working group data
    * @param forProjectId vidispine project ID to look up
    * @param existingMxsMeta CustomMXSMetadata object representing the current state of file metadata
    * @return a Future containing a copy of `existingMxsMeta` that has been updated with known fields
    */
  def lookupProjectAndCommission(forProjectId:String, existingMxsMeta:CustomMXSMetadata) = {
    val projectFut = (plutoCommunicator ? LookupProject(forProjectId)).mapTo[AFHMsg]
    projectFut.flatMap({
      case LookupFailed => Future.failed(new RuntimeException("Lookup failed, see previous logs"))
      case FoundProject(Some(project)) =>
        //fixme: the site id is a cheat but will work for now
        val commissionFut = (plutoCommunicator ? LookupCommission(s"VX-${project.commission}")).mapTo[AFHMsg]
        commissionFut.flatMap({
          case LookupFailed => Future.failed(new RuntimeException("Lookup failed, see previous logs"))
          case FoundCommission(Some(commission)) =>
            val workingGroupFut = (plutoCommunicator ? LookupWorkingGroup(commission.gnm_commission_workinggroup)).mapTo[AFHMsg]
            workingGroupFut.flatMap({
              case LookupFailed => Future.failed(new RuntimeException("Lookup failed, see previous logs"))
              case FoundWorkingGroup(Some(workingGroup)) =>
                Future(existingMxsMeta.copy(
                  projectId = Some(forProjectId),
                  commissionId = commission.collection_id.map(_.toString),
                  projectName = project.gnm_project_headline,
                  commissionName = Some(commission.gnm_commission_title),
                  workingGroupName = Some(workingGroup.name))
                )
              case FoundWorkingGroup(None) =>
                Future(existingMxsMeta.copy(
                  projectId = Some(forProjectId),
                  commissionId = Some(commission.collection_id.toString),
                  projectName = project.gnm_project_headline,
                  commissionName = Some(commission.gnm_commission_title)
                ))
            })
          case FoundCommission(None) =>
            Future(existingMxsMeta.copy(projectId = Some(forProjectId), projectName = project.gnm_project_headline))
        })
      case FoundProject(None) =>
        logger.warn(s"No project found for ID $forProjectId (got from asset folder db)")
        Future(existingMxsMeta.copy(projectId = Some(forProjectId)))
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

  def lookupAllMetadataForMasters(masterFile:Path, existingMxsMeta:CustomMXSMetadata)(implicit logger:Logger) = {
    val masterFut = (plutoCommunicator ? LookupMaster(masterFile.getFileName.toString)).mapTo[AFHMsg]

    masterFut.flatMap({
      case LookupFailed=>Future.failed(new RuntimeException("Lookup failed, see previous logs"))
      case FoundMaster(masterRecords)=>
        if(masterRecords.length>1){
          logger.warn(s"Got ${masterRecords.length} records for $masterFile, expected 1. Using the first.")
        }

        masterRecords.headOption match {
          case Some(masterRecord)=>
            lookupProjectAndCommission(s"VX-${masterRecord.project}", existingMxsMeta).map(updatedMeta=>
              //FIXME: not got master ID yet!
              updatedMeta.copy(masterName=Some(masterRecord.title), masterUser = Some(masterRecord.user.toString))
            )
          case None=>
            logger.warn(s"Got no master record for $masterFile")
            Future(existingMxsMeta)
        }
    })
  }

  def lookupAllMetadataForDeliverables(deliverablePath:Path, existingMxsMeta:CustomMXSMetadata)(implicit logger:Logger) = {
    val assetFut = (plutoCommunicator ? LookupDeliverableAsset(deliverablePath.getFileName.toString)).mapTo[AFHMsg]

    assetFut.flatMap({
      case LookupFailed=>Future.failed(new RuntimeException("Lookup failed, see previous logs"))
      case FoundDeliverableAsset(Some(record))=>
        val delivFut = (plutoCommunicator ? LookupDeliverableBundle(record.deliverable)).mapTo[AFHMsg]
        delivFut.flatMap({
          case LookupFailed=>Future.failed(new RuntimeException("Lookup failed, see previous logs"))
          case FoundDeliverableBundle(Some(bundle))=>
            lookupProjectAndCommission(bundle.project_id, existingMxsMeta).map(updatedMeta=>
              updatedMeta.copy(
                deliverableBundle = Some(record.deliverable),
                deliverableType = record.type_string,
                deliverableVersion = record.version,
                deliverableAssetId = Some(record.id)
              )
            )
          case FoundDeliverableBundle(None)=>
            logger.warn(s"Got a deliverable record referring to a non-existing bundle ${record.deliverable}")
            Future(existingMxsMeta.copy(
              deliverableBundle = Some(record.deliverable),
              deliverableType = record.type_string,
              deliverableVersion = record.version,
              deliverableAssetId = Some(record.id)
            ))
        })
      case FoundDeliverableAsset(None)=>
        logger.warn(s"Got no deliverable asset for path ${deliverablePath.toString}")
        Future(existingMxsMeta) //we did not find any record so we can't update anything
    })
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private implicit val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    val completedCb = createAsyncCallback[BackupEntry](e=>push(out,e))
    val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val maybeMetadata = elem.maybeObjectMatrixEntry.flatMap(_.attributes).flatMap(CustomMXSMetadata.fromMxsMetadata)
        val basePath = elem.originalPath.getParent

        logger.info(s"basePath is $basePath")

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
              case CustomMXSMetadata.TYPE_MASTER=>
                lookupAllMetadataForMasters(elem.originalPath, currentMeta)
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
          case Failure(err)=>
            logger.error(s"Could not look up metadata for ${elem.originalPath}: ", err)
            failedCb.invoke(err)
        })
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

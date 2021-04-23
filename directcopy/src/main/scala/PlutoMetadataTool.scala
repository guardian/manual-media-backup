import akka.actor.ActorRef
import helpers.PlutoCommunicator.{AFHMsg, FoundAssetFolder, FoundCommission, FoundProject, FoundWorkingGroup, Lookup, LookupCommission, LookupFailed, LookupProject, LookupWorkingGroup}
import models.{CustomMXSMetadata, MxsMetadata, ToCopy}
import akka.pattern.ask
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class PlutoMetadataTool(plutoCommunicator:ActorRef) {
  implicit val timeout:akka.util.Timeout = 30.seconds
  private val logger = LoggerFactory.getLogger(getClass)

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
    * the main method for this helper. Called from a mapAsync, this will try to add the relevant pluto metadata onto the
    * given [[ToCopy]] instance
    * @param incomingEntry [[ToCopy]] instance indicating the files that will be copied
    */
  def addPlutoMetadata(incomingCopyRequest:ToCopy):Future[ToCopy] = {
    val basePath = incomingCopyRequest.sourceFile.path.getParent

    val assetFolderFut = (plutoCommunicator ? Lookup(basePath) ).mapTo[AFHMsg]
    assetFolderFut.flatMap({
      case LookupFailed=>
        Future.failed(new RuntimeException("Lookup failed, see previous logs"))
      case FoundAssetFolder(Some(assetFolder))=>
        logger.info(s"Got asset folder ${assetFolder.path} for ${assetFolder.project} for $basePath")
        val existingMetadata =
          incomingCopyRequest.commonMetadata
            .flatMap(CustomMXSMetadata.fromMxsMetadata)
            .getOrElse(CustomMXSMetadata.empty(CustomMXSMetadata.TYPE_RUSHES))

        lookupProjectAndCommission(assetFolder.project, existingMetadata)
          .map(customMxsMetadata=>{
            incomingCopyRequest.copy(
              commonMetadata=Some(
                customMxsMetadata.toAttributes(
                  incomingCopyRequest.commonMetadata.getOrElse(MxsMetadata.empty())
                )
              )
            )
          })

      case FoundAssetFolder(None)=>
        logger.info(s"Got no asset folder for $basePath")
        Future(incomingCopyRequest)
    })
  }
}

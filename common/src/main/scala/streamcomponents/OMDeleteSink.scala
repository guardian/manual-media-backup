package streamcomponents

import akka.Done
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic, GraphStageWithMaterializedValue}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import models.ObjectMatrixEntry
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scala.util.Success

class OMDeleteSink(userInfo:UserInfo, reallyDelete:Boolean) extends GraphStageWithMaterializedValue[SinkShape[ObjectMatrixEntry], Future[Done]]{
  private final val in:Inlet[ObjectMatrixEntry] = Inlet.create("OMDeleteSink.in")

  override def shape: SinkShape[ObjectMatrixEntry] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val completionPromise = Promise[Done]()

    val logic = new GraphStageLogic(shape) {
      private val logger = LoggerFactory.getLogger(getClass)

      private var maybeVault:Option[Vault] = None

      setHandler(in, new AbstractInHandler {
        override def onPush(): Unit = {
          if(maybeVault.isEmpty) {
            failStage(new RuntimeException("Received data when no vault connection was present, this should not happen"))
            return
          }

          val vault = maybeVault.get
          val elem = grab(in)

          try {
            val item = vault.getObject(elem.oid)
            if(item.exists()){
              if(reallyDelete) {
                logger.info(s"Deleting item ${elem.oid} (${elem.pathOrFilename.getOrElse("no filename")})...")
                item.delete()
              } else {
                logger.info(s"I would delete item ${elem.oid} (${elem.pathOrFilename.getOrElse("no filename")})...")
              }
            } else {
              logger.warn(s"Requested to delete item ${elem.oid} (${elem.pathOrFilename.getOrElse("no filename")}) that does not exist on the vault")
            }
          } catch {
            case err:Throwable=>
              logger.error(s"Could not check or delete ${elem.oid} (${elem.pathOrFilename.getOrElse("no filename")}): ", err)
              failStage(err)
          }
        }
      })

      override def preStart(): Unit = {
        try {
          maybeVault = Some(MatrixStore.openVault(userInfo))
          pull(in)
        } catch {
          case err:Throwable=>
            logger.error(s"Could not attach to vault ${userInfo.getVault} on ${userInfo.getAddresses}: ", err)
            failStage(err)
        }
      }

      override def afterPostStop() = {
        maybeVault.map(_.dispose())
        completionPromise.complete(Success(Done))
      }
    }

    (logic, completionPromise.future)
  }
}

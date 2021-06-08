import com.om.mxs.client.japi.{Attribute, AttributeView, MatrixStore, ObjectTypedAttributeView, UserInfo, Vault}
import models.ToCopy
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

object ProxyLinker {
  val ProxyField = "ATT_PROXY_OID"
  val ThumbnailField = "ATT_THUMB_OID"
  val MetaField = "ATT_META_OID"

  def initialise(destVaultConnector:MXSConnectionBuilder) = {
    destVaultConnector.build().flatMap(mxs=>Try {
      val vault = mxs.openVault(destVaultConnector.vaultId)
      new ProxyLinker(vault)
    })
  }
}

class ProxyLinker (destVault:Vault) {
  import ProxyLinker._
  import cats.implicits._
  private val logger = LoggerFactory.getLogger(getClass)

  override def finalize(): Unit = {
    destVault.dispose()
    super.finalize()
  }

  protected def makeUpdate(name:String, oid:String) =
    new Attribute(name, oid)

  protected def getOMFile(oid:String) = Try { destVault.getObject(oid) }
  protected def writeOMAttr(view:ObjectTypedAttributeView,attr:Attribute) = Try {view.writeAttribute(attr)}

  /**
    * call the OM SDK to write the provided attributes to the provided view
    * @param view ObjectTypedAttributeView from the file whose attributes are to be written
    * @param toWrite a List of Attribute instances containing the information to write
    * @return a Try, containing the number of attributes written. This fails if there is any write error.
    */
  protected def writeAttributesToView(view:ObjectTypedAttributeView, toWrite:Vector[Attribute], attempt:Int=0):Try[Int] = {
    def internalWrite =
      toWrite
        .map(writeOMAttr(view, _))
        .sequence
        .map(_.length)


    internalWrite match {
      case ok@Success(_)=>ok
      case problem@Failure(err)=>
        if(err.getMessage.contains("error 311")) {
          logger.warn(s"Could not write attributes on attempt $attempt, got locking error. Retrying in 5s...")
          Thread.sleep(5000)
          writeAttributesToView(view, toWrite, attempt+1)
        } else {
          logger.error(s"Could not write attributes: ${err.getMessage}", err)
          problem
        }
    }
  }

  def performLinkup(copied:ToCopy):Future[ToCopy] = Future.fromTry({
    copied.sourceFile.oid match {
      case None=>
        logger.warn(s"Can't do proxy link for ${copied.sourceFile.path}, as no copy was performed")
        Success(copied)
      case Some(sourceOid)=>
        logger.debug(s"${copied.sourceFile.path.toString}: Source OID is $sourceOid, proxy OID is ${copied.proxyMedia.flatMap(_.oid)}, thumbnail OID is ${copied.thumbnail.flatMap(_.oid)}")
        val potentialUpdates = Vector(
          copied.proxyMedia
            .flatMap(_.oid)
            .map(makeUpdate(ProxyField, _)),
          copied.thumbnail
            .flatMap(_.oid)
            .map(makeUpdate(ThumbnailField, _))
        )

        val updatesToMake = potentialUpdates.collect({case Some(attr)=>attr})
        logger.debug(s"${copied.sourceFile.path.toString}: copied source file is $sourceOid, updates to make are $updatesToMake")
        if(updatesToMake.nonEmpty) {
          val updatesMade = for {
            sourceObject <- getOMFile(sourceOid)
            attrView <- Try {
              sourceObject.getAttributeView
            }
            resultCount <- writeAttributesToView(attrView, updatesToMake)
          } yield resultCount

          updatesMade.map(updateCount=>{
            logger.info(s"Updated $updateCount fields on ${copied.sourceFile.oid} (${copied.sourceFile.path})")
            copied
          })
        } else {
          Success(copied)
        }
    }
  })
}

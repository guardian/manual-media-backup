package streamcomponents

import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.om.mxs.client.japi.{Constants, MatrixStore, SearchTerm, UserInfo}
import models.ObjectMatrixEntry
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
  * performs a "FastSearch" using a provided query string for Content Search mode
  * @param userInfo UserInfo object identifying the appliance, vault and credentials to use. Each instance of the stage maintains its own connection
  * @param queryString querystring for the Content Search. See objectmatrix's documentation for details
  * @param atOnce how many objects to return per batch. Defaults to 10.
  */
class OMFastContentSearchSource(userInfo:UserInfo, queryString:String, atOnce:Int=10) extends GraphStage[SourceShape[ObjectMatrixEntry]] {
  private final val out:Outlet[ObjectMatrixEntry] = Outlet.create("OMFastSearchSource.out")

  override def shape: SourceShape[ObjectMatrixEntry] = SourceShape.of(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new OMFastSearchCommonLogic(shape) {
    override protected val logger: Logger= LoggerFactory.getLogger(getClass)

    setHandler(out, standardOutHandler(out))

    override def preStart(): Unit = {
      //establish connection to OM
      try {
        logger.debug("OMFastContentSearchSource starting up")
        logger.info(s"Establishing connection to ${userInfo.getVault} on ${userInfo.getAddresses} as ${userInfo.getUser}")
        vault = Some(MatrixStore.openVault(userInfo))

        val finalTerm = SearchTerm.createSimpleTerm(Constants.CONTENT, queryString)
        iterator = vault.map(_.searchObjectsIterator(finalTerm, atOnce).asScala)
        logger.info("Connection established")

      } catch {
        case ex: Throwable =>
          logger.error(s"Could not establish connection: ", ex)
          failStage(ex)
      }
    }
  }
}

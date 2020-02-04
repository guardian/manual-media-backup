package streamcomponents

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{MatrixStore, SearchTerm, UserInfo}
import models.{MxsMetadata, ObjectMatrixEntry}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class OMFastSearchSource(userInfo:UserInfo, searchTerms:Array[SearchTerm], includeFields:Array[String], contentSearchBareTerm:Boolean=false, atOnce:Int=10) extends GraphStage[SourceShape[ObjectMatrixEntry]] {
  private final val out:Outlet[ObjectMatrixEntry] = Outlet.create("OMFastSearchSource.out")

  override def shape: SourceShape[ObjectMatrixEntry] = SourceShape.of(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new OMFastSearchCommonLogic(shape) {
    override protected val logger: Logger= LoggerFactory.getLogger(getClass)

    setHandler(out, standardOutHandler(out))
    override def preStart(): Unit = {
      //establish connection to OM
      try {
        logger.debug("OMFastSearchSource starting up")
        logger.info(s"Establishing connection to ${userInfo.getVault} on ${userInfo.getAddresses} as ${userInfo.getUser}")
        vault = Some(MatrixStore.openVault(userInfo))

        val finalTerm = if(contentSearchBareTerm) {
          searchTerms.head
        } else {
          SearchTerm.createANDTerm(searchTerms ++ includeFields.map(field=>SearchTerm.createSimpleTerm("__mxs__rtn_attr", field)))
        }
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

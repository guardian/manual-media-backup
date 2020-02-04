package streamcomponents

import akka.stream.{Graph, Outlet, Shape}
import akka.stream.stage.{AbstractOutHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.Vault
import models.{MxsMetadata, ObjectMatrixEntry}

abstract class OMFastSearchCommonLogic(s:Shape) extends GraphStageLogic(s) {
  protected val logger:org.slf4j.Logger

  def parseOutResults(resultString:String) = {
    logger.debug(s"parseOutResults: got $resultString")
    val parts = resultString.split("\n")

    val kvs = parts.tail
      .map(_.split("="))
      .foldLeft(Map[String,String]()) ((acc,elem)=>acc ++ Map(elem.head -> elem.tail.mkString("=")))
    logger.debug(s"got $kvs")
    val mxsMeta = MxsMetadata(kvs,Map(),Map(),Map(),Map())

    logger.debug(s"got $mxsMeta")
    ObjectMatrixEntry(parts.head,attributes = Some(mxsMeta), fileAttribues = None)
  }

  protected var vault: Option[Vault] = None
  protected var iterator: Option[Iterator[String]] = None

  def standardOutHandler(out:Outlet[ObjectMatrixEntry]) = new AbstractOutHandler {
    override def onPull(): Unit = {
      iterator match {
        case None =>
          logger.error(s"Can't iterate before connection was established")
          failStage(new RuntimeException)
        case Some(iter) =>
          if (iter.hasNext) {
            val resultString = iter.next()
            val elem = parseOutResults(resultString)
            logger.debug(s"Got element $elem")
            push(out, elem)
          } else {
            logger.info(s"Completed iterating results")
            complete(out)
          }
      }
    }
  }

  override def postStop(): Unit = {
    logger.info("Search stream stopped")
    vault.map(_.dispose())
  }
}

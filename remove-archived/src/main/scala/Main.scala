import akka.stream.ClosedShape
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.{Merge, Sink}
import com.om.mxs.client.japi.{Attribute, Constants, MatrixStore, MxsObject, SearchTerm, UserInfo, Vault}
import models.{ArchiveStatus, PotentialRemoveStreamObject}
import streamcomponents.{LocalFileExists, OMFastSearchSource}

object Main {
  def buildStream(userInfo:UserInfo) = {
    val terms = Array(SearchTerm.createSimpleTerm(Constants.CONTENT,"*"))
    val includeFields = Array("MXFS_PATH","MXFS_FILENAME")

    val sinkFact = Sink.seq[PotentialRemoveStreamObject]

    GraphDSL.create(sinkFact) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new OMFastSearchSource(userInfo,terms,includeFields))
      val existsSwitch = builder.add(new LocalFileExists)
      val finalMerger = builder.add(Merge[PotentialRemoveStreamObject](4, false))

      src.out.map(PotentialRemoveStreamObject.apply) ~> existsSwitch
      existsSwitch.out(0).map(_.copy(status = Some(ArchiveStatus.SHOULD_KEEP))) ~> finalMerger      //YES branch => file does exist on primary, not ready to remove

      existsSwitch.out(1) ~>

      finalMerger ~> sink
      ClosedShape
    }
  }
}

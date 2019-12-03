package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import io.circe.CursorOp
import models.{BackupEntry, CustomMXSMetadata, MxsMetadata, ObjectMatrixEntry}
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

/**
  * tries to set the GNM_TYPE field based on the path
  */
class AddTypeField(pathDefinitionsFile:String) extends GraphStage[FlowShape[BackupEntry, BackupEntry ]] {
  private final val in:Inlet[BackupEntry] = Inlet.create("AddTypeField.in")
  private final val out:Outlet[BackupEntry] = Outlet.create("AddTypeField.out")

  override def shape: FlowShape[BackupEntry, BackupEntry] = FlowShape.of(in,out)

  def loadKnownPaths() = {
    Try { Source.fromFile(pathDefinitionsFile,"UTF-8") } match {
      case Success(s) =>
        try {
          val definitionsContent = s.mkString
          val definitions = io.circe.parser.parse(definitionsContent)
          val rawMapOrError = definitions.flatMap(_.as[Map[String, String]])

          rawMapOrError.map(_.map(kv => (kv._1.r, kv._2)))
        } finally {
          s.close()
        }
      case Failure(err)=>Left(io.circe.DecodingFailure(s"Could not read file: $err",List()))
    }
  }

  /**
    * sets the GNM_TYPE tag on the item to the given value.
    * does not write it down to the storage yet, just performs the add in-memory
    * @param addTo ObjectMatrixEntry to add to
    * @param tagToAdd tag value to set
    * @return the updated ObjectMatrixEntry.  If the previous one had no metadata then new metadata is added.
    */
  def addTag(addTo:ObjectMatrixEntry, tagToAdd:String):ObjectMatrixEntry = {
    val existingMetadata = addTo.attributes.getOrElse(MxsMetadata.empty())
    val updatedMetadata = existingMetadata.withString("GNM_TYPE", tagToAdd)
    addTo.copy(attributes = Some(updatedMetadata))
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)
    private var knownPaths:Option[Map[Regex, String]] = None

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val pathToCheck = elem.originalPath.toString
        val matches = knownPaths.get.filter(kv=>kv._1.findFirstIn(pathToCheck).isDefined)
        if(elem.maybeObjectMatrixEntry.isEmpty){
          logger.error(s"Can't add type tag if there is no objectmatrix entry present. Aborting.")
          failStage(new RuntimeException("No objectmatrix entry present"))
          return
        }
        val updatedElem = matches.headOption match {
          case None=>
            logger.info(s"Path $pathToCheck did not match any known location, tagging as '${CustomMXSMetadata.TYPE_UNSORTED}'")
            elem.copy(maybeObjectMatrixEntry = elem.maybeObjectMatrixEntry.map(entry=>addTag(entry,CustomMXSMetadata.TYPE_UNSORTED)))
          case Some(locationMatch)=>
            logger.info(s"Path $pathToCheck matched location for ${locationMatch._2}")
            elem.copy(maybeObjectMatrixEntry = elem.maybeObjectMatrixEntry.map(entry=>addTag(entry,locationMatch._2)))
        }
        push(out, updatedElem)
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })

    override def preStart(): Unit = {
      loadKnownPaths() match {
        case Left(err)=>
          logger.error(s"Could not load known paths definitions from '$pathDefinitionsFile': $err")
          throw new RuntimeException(err.toString)
        case Right(defs)=>
          knownPaths = Some(defs)
      }
    }
  }
}

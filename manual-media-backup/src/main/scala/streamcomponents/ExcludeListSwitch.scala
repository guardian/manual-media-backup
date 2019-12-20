package streamcomponents

import java.nio.file.Path

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import org.slf4j.LoggerFactory

/**
  * filters out paths that match anything in the given list of regexes
  * @param excludeFilesPath
  */
class ExcludeListSwitch(excludeFilesPath:Option[String]) extends GraphStage[FlowShape[Path,Path]]{
  private final val in:Inlet[Path] = Inlet("ExcludeListSwitch.in")
  private final val out:Outlet[Path] = Outlet("ExcludeListSwitch.out")

  private val outerLogger = LoggerFactory.getLogger(getClass)
  def loadPathsList = excludeFilesPath.map(pathsFileName=>{
    val fileContent = scala.io.Source.fromFile(pathsFileName, "UTF-8")
    io.circe.parser.parse(fileContent.mkString).flatMap(json=> {
      json.asArray match {
        case None => Left(io.circe.ParsingFailure("Expected a single array/list but got something else",new RuntimeException("wrong content")))
        case Some(arrayContent) =>
          val results = arrayContent.map(_.as[String])
          val failures = results.collect({ case Left(err) => err })
          if (failures.nonEmpty) {
            outerLogger.error(s"${failures.length} strings did not parse: ")
            failures.foreach(err=>outerLogger.error(s"\t${err.toString()}"))
            Left(failures.head)
          } else {
            Right(results.collect({ case Right(content) => content.r }))
          }
      }
    })
  })

  lazy val pathsListContent = loadPathsList

  override def shape: FlowShape[Path, Path] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem=grab(in)
        pathsListContent match {
          case None=>
            push(out, elem)
          case Some(Right(regexList))=>
            val matches = regexList.filter(re=>re.findFirstIn(elem.toString).isDefined)
            if(matches.nonEmpty){
              logger.info(s"Path ${elem.toString} matched (at least) ${matches.head.regex}")
              pull(in)
            } else {
              push(out, elem)
            }
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })

    override def preStart(): Unit = {
      pathsListContent match {
        case Some(Left(parsingError))=>
          logger.error(s"Could not initialise ExcludeListSwitch, ${excludeFilesPath} did not parse: ${parsingError}")
          failStage(new RuntimeException("exclude list did not parse"))
        case _=>
      }
    }
  }
}

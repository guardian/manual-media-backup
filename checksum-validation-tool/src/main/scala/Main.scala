import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink}
import com.om.mxs.client.japi.{MatrixStore, UserInfo}
import helpers.MatrixStoreHelper
import org.slf4j.LoggerFactory
import streamcomponents.{FixChecksumField, OMFastContentSearchSource}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val actorSystem = ActorSystem("manual-media-backup")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  def buildOptionParser = new scopt.OptionParser[Options]("checksum-validation-tool") {
    head("find-filename", "1.x")
    opt[String]("vault-file").action((x,c)=>c.copy(vaultFile = x)).text(".vault file from MatrixStore Admin to provide credentials")
    opt[String]("path").action((x,c)=>c.copy(searchPath = x)).text("path to search for")
  }

  def buildValidationGraph(userInfo:UserInfo) = {
    val queryString =
      """*
        |keywords: MXFS_PATH,__mxs__length,__mxs__calc_md5,MXFS_FILENAME
        |""".stripMargin
    val sinkFactory = Sink.ignore
    GraphDSL.create(sinkFactory) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new OMFastContentSearchSource(userInfo,queryString,atOnce=10))
      val csfix = builder.add(new FixChecksumField)

      src ~> csfix
      csfix.out
          .mapAsyncUnordered(10)(entry=>Future {
            val applianceChecksum = entry.attributes.flatMap(_.stringValues.get("__mxs__calc_md5"))
            LocalChecksum.findFile(entry) match {
              case Some(f)=>
                LocalChecksum.calculateChecksum(f) match {
                  case Success(localChecksum)=>
                    (entry.maybeGetFilename().get, applianceChecksum, localChecksum)
                  case Failure(err)=>
                    logger.error(s"Unable to calculate checksum for ${f.toString}: ", err)
                    throw err
                }
              case None=>
                logger.error(s"Could find local file for ${entry.maybeGetFilename().getOrElse("(no name)")} (${entry.oid})")
                (entry.maybeGetFilename().getOrElse("(no name)"), applianceChecksum, "")
            }
          })
        .map(elem=>{
          logger.info(s"${elem._1} => appliance ${elem._2}, local ${elem._3}")
        }) ~> sink
      ClosedShape
    }
  }

  def main(args: Array[String]): Unit = {
    buildOptionParser.parse(args,Options()) match {
      case Some(parsedArgs)=>
        UserInfoBuilder.fromFile(parsedArgs.vaultFile) match {
          case Success(userInfo)=>
            val graph = buildValidationGraph(userInfo)
            RunnableGraph.fromGraph(graph).run().onComplete({
              case Success(_)=>
                logger.info("Run completed")
                actorSystem.terminate()
              case Failure(err)=>
                logger.error("Run crashed: ", err)
                actorSystem.terminate()
            })
          case Failure(err)=>
            logger.error("Could not initialise: ", err)
            actorSystem.terminate()
        }
      case None=>
        println("Nothing to do!")
        actorSystem.terminate()
    }
  }
}

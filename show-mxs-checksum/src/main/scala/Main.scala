import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink}
import com.om.mxs.client.japi.{MatrixStore, SearchTerm, UserInfo, Vault}
import org.slf4j.LoggerFactory
import streamcomponents.OMSearchSource

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val actorSystem = ActorSystem("vs-media-backup")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  lazy val vaultFile = sys.env.get("VAULT_FILE") match {
    case Some(f)=>f
    case None=>
      logger.error("You must specify VAULT_FILE in the environment")
      Await.ready(terminate(1), 1 hour)
      throw new RuntimeException("Could not shut down actorsystem within time limit")
  }

  /**
    * asynchronously shuts down the actorsystem and then terminates the JVM session with the given exitcode
    * @param exitCode exitcode to return to system
    * @return a Future, which should effectively never resolve (JVM should quit as it does)
    */
  def terminate(exitCode:Int) = actorSystem.terminate().andThen({
    case _=>System.exit(exitCode)
  })


  def buildStream(userInfo:UserInfo,vault:Vault, searchParams:Option[SearchTerm]) = {
    val sinkFactory = Sink.ignore

    GraphDSL.create(sinkFactory) {implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new OMSearchSource(userInfo, searchParams,None))
      val showCS = builder.add(new ShowMxsChecksum(vault))

      src ~> showCS ~> sink
      ClosedShape
    }
  }

  def main(args:Array[String]):Unit = {
    val userInfo = UserInfoBuilder.fromFile(vaultFile) match {
      case Success(info)=>info
      case Failure(err)=>
        logger.error(s"Could not get login data from vault file '$vaultFile'", err)
        Await.ready(terminate(1), 1 hour)
        throw new RuntimeException("Could not shut down actorsystem within time limit")
    }

    val vault = MatrixStore.openVault(userInfo)

    //None in search terms means get everything
    RunnableGraph.fromGraph(buildStream(userInfo, vault, None)).run().onComplete({
      case Failure(err)=>
        logger.error("Main stream failed: ", err)
        terminate(1)
      case Success(_)=>
        logger.info("Completed iterating results")
        terminate(0)
    })
  }
}

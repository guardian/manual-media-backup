import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.om.mxs.client.japi.{MatrixStore, UserInfo}
import org.slf4j.LoggerFactory
import helpers._
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object Main {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val actorSystem = ActorSystem("inspect-oid")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  /**
    * asynchronously shuts down the actorsystem and then terminates the JVM session with the given exitcode
    * @param exitCode exitcode to return to system
    * @return a Future, which should effectively never resolve (JVM should quit as it does)
    */
  def terminate(exitCode:Int) = actorSystem.terminate().andThen({
    case _=>System.exit(exitCode)
  })

  def buildOptionParser = {
    new scopt.OptionParser[Options]("inspect-oid") {
      head("inspect-oid", "1.x")

      opt[String]("vault-file").action((x,c)=>c.copy(vaultFile = Some(x))).text(".vault file from ObjectMatrix Admin that describes the cluster, vault and login details. This is provided when you create the vault.")
      opt[String]("oid").action((x,c)=>c.copy(oid = Some(x))).text("look up this filepath on the provided ObjectMatrix")
      opt[Boolean]("delete").action((x,c)=>c.copy(delete=x)).text("delete the file from the objectmatrix appliance")
    }
  }

  def lookupOid(info: UserInfo, oid: String, delete:Boolean): Future[Unit] ={
    val vault = MatrixStore.openVault(info)

    Try { vault.getObject(oid) } match {
      case Success(mxsFile)=>
        if(mxsFile.exists()) {
          logger.info(s"Found file ${mxsFile.getId}")

          val mxfsMeta = Option(MetadataHelper.getMxfsMetadata(mxsFile))
          logger.info(s"mxfsMeta is $mxfsMeta")
          logger.info(s"${mxsFile.getId}: ${mxfsMeta.map(_.fileKey())} ${mxfsMeta.map(_.getName)} ${mxfsMeta.map(_.getParent)} ${mxfsMeta.map(_.creationTime())} ${mxfsMeta.map(_.size())}")

          MetadataHelper.getAttributeMetadata(mxsFile).map(attribs => {
            logger.info(s"${mxsFile.getId}: $attribs")
          }).map(_ => {
            if (delete) {
              logger.warn(s"Deleting $oid because I was asked to")
              mxsFile.delete()
            }
          })
        } else {
          logger.info(s"File ${mxsFile.getId} does not exist on vault ${vault.getId}")
          Future(())
        }
    }
  }

  def main(args:Array[String]):Unit = {
    buildOptionParser.parse(args, Options()) match {
      case Some(options)=>
        if(options.vaultFile.isEmpty || options.oid.isEmpty){
          logger.error("You must specify the --vault-file and --oid options")
          Await.ready(terminate(1), 2 hours)
        }

        UserInfoBuilder.fromFile(options.vaultFile.get) match {
          case Success(userInfo)=>
            lookupOid(userInfo, options.oid.get, options.delete).andThen({
              case Failure(err)=>
                logger.error(s"Could not lookup: ", err)
                terminate(2)
              case Success(_)=>
                logger.info(s"Completed")
                terminate(0)
            })
          case Failure(err)=>
            logger.error(s"Could not read vault file ${options.vaultFile.get}: ", err)
            terminate(3)
        }
      case None=>
        //message should have been displayed if there are no arguments
        terminate(1)
    }
  }
}

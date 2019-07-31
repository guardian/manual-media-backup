import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, Merge, RunnableGraph, Sink, Source}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import models.{CopyReport, VSBackupEntry, VSConfig}
import org.slf4j.LoggerFactory
import vidispine.VSCommunicator
import vsStreamComponents.{CreateFileDuplicate, DecodeMediaCensusOutput, LookupFullPath, LookupVidispineMD5, VSCloseFile, VSDeleteFile, VSListCopyFile}
import com.softwaremill.sttp._
import streamcomponents.{ValidateMD5, ValidationSwitch}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val actorSystem = ActorSystem("vs-media-backup")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  /**
    * asynchronously shuts down the actorsystem and then terminates the JVM session with the given exitcode
    * @param exitCode exitcode to return to system
    * @return a Future, which should effectively never resolve (JVM should quit as it does)
    */
  def terminate(exitCode:Int) = actorSystem.terminate().andThen({
    case _=>System.exit(exitCode)
  })

  lazy val vsConfig = VSConfig(
    uri"${sys.env("VIDISPINE_BASE_URL")}",
    sys.env("VIDISPINE_USER"),
    sys.env("VIDISPINE_PASSWORD")
  )

  lazy val chunkSize = sys.env.getOrElse("CHUNK_SIZE","1024").toInt //chunk size in kByte/s

  lazy implicit val vsCommunicator = new VSCommunicator(vsConfig.vsUri, vsConfig.plutoUser, vsConfig.plutoPass)

  lazy val storageId:String = sys.env.get("DEST_STORAGE_ID") match {
    case Some(str)=>str
    case None=>
      logger.error("You must specify DEST_STORAGE_ID in the environment")
      Await.ready(terminate(1), 1 hour)
      ""  //we should never reach this point
  }

  lazy val vaultFile:String = sys.env.get("VAULT_FILE") match {
    case Some(str)=>str
    case None=>
      logger.error("You must specify VAULT_FILE to point to a .vault file with login credentials for the target vault")
      Await.ready(terminate(1), 1 hour)
      "" // we should never reach this point
  }

  def firstTestStream(dataSource:String, vault:Vault, userInfo:UserInfo) = {
    val sinkFactory = Sink.fold[Seq[CopyReport[VSBackupEntry]],CopyReport[VSBackupEntry]](Seq())((acc,elem)=>acc++Seq(elem))
    GraphDSL.create(sinkFactory) {implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new DecodeMediaCensusOutput(dataSource))
      val pathLookup = builder.add(new LookupFullPath(vsCommunicator))              //find the filesystem storage location we copy from
      val vsLookup = builder.add(new LookupVidispineMD5(vsCommunicator))            //look up VS checksum and size
      val copier = builder.add(new VSListCopyFile(userInfo, vault, chunkSize*1024)) //perform the copy
      //val validator = builder.add(new ValidateMD5[VSBackupEntry](vault))
      //val vsDeleteCorruptFile = builder.add(new VSDeleteFile(vsCommunicator,storageId))
      //val validationSwitch = builder.add(new ValidationSwitch[VSBackupEntry](treatNoneAsSuccess = true))

      val duper = builder.add(new CreateFileDuplicate(vsCommunicator, storageId, dryRun = false)) //create a file record in VS marked as a duplicate
      val closer = builder.add(new VSCloseFile(vsCommunicator, storageId))                        //ensure that the created file is marked as "closed"
      //val merge = builder.add(Merge[CopyReport[VSBackupEntry]](2))

      src ~> pathLookup ~> vsLookup ~> duper ~> copier ~> closer ~> sink //~> validator ~> validationSwitch
//      validationSwitch.out(0) ~> merge //we validated correctly
//      validationSwitch.out(1) ~> vsDeleteCorruptFile ~> merge  //we did not validate correctly, so delete the corrupt desintation record (which should in turn delete the file)
//      merge ~> sink
      ClosedShape
    }
  }

  def readResourceFile(name:String) = {
    val inputStream = getClass.getResourceAsStream("testdata.json")

    val src = scala.io.Source.fromInputStream(inputStream)
    val content = src.mkString
    inputStream.close()
    content
  }

  def main(args:Array[String]):Unit = {
    val testContent = readResourceFile("testdata.json")

    val userInfo = UserInfoBuilder.fromFile(vaultFile) match {
      case Failure(err)=>
        logger.error(s"Could not load user info from '$vaultFile': ", err)
        Await.ready(terminate(2), 1 hour)
        throw new RuntimeException("could not terminate actor system in time limit")  //we should not get here, this is just to keep the return value correct.
      case Success(info)=>info
    }

    val vault = MatrixStore.openVault(userInfo)

    val graph = firstTestStream(testContent,vault, userInfo)

    RunnableGraph.fromGraph(graph).run().onComplete({
      case Failure(err)=>
        logger.error(s"Stream failed: ", err)
        terminate(1)
      case Success(results)=>
        logger.info("Stream completed.")
        results.foreach(entry=>logger.info(s"\tOutput: $entry"))
        terminate(0)
    })
  }
}

import helpers.MatrixStoreHelper
import com.om.mxs.client.japi.MatrixStore

import scala.util.{Failure, Success}

object Main {
  def buildOptionParser = new scopt.OptionParser[Options]("find-filename") {
    head("find-filename", "1.x")
    opt[String]("vault-file").action((x,c)=>c.copy(vaultFile = Some(x))).text("(deprecated) .vault file from MatrixStore Admin to provide credentials")
    opt[String]("credentials").action((x,c)=>c.copy(credentialsFile = Some(x))).text(".yaml format credentials file for the MatrixStore")
    opt[String]("path").action((x,c)=>c.copy(searchPath = x)).text("path to search for")
  }

  def main(args: Array[String]): Unit = {
    buildOptionParser.parse(args,Options()) match {
      case Some(parsedArgs)=>
        val maybeVault = (parsedArgs.vaultFile, parsedArgs.credentialsFile) match {
          case (_, Some(credentialsFile))=>
            MXSConnectionBuilder.fromYamlFile(credentialsFile) match {
              case Left(err)=>
                println(s"Could not load credentials from '$credentialsFile': $err")
                sys.exit(2)
              case Right(connectionInfo)=>
                connectionInfo.build().map(mxs=>{
                    sys.addShutdownHook(()=>{
                      println("Disconnecting from MatrixStore")
                      mxs.closeConnection()
                      mxs.dispose()
                    })
                    mxs.openVault(connectionInfo.vaultId)
                })
            }
          case (Some(vaultFile), _)=>
            UserInfoBuilder.fromFile(vaultFile).map(userInfo=>MatrixStore.openVault(userInfo))
          case (None, None)=>
            println("You must specify either --vault-file or --credentials on the commandline")
            sys.exit(1)
        }

        val maybeResults = parsedArgs.searchPath match {
          case Some(searchPath)=>
            maybeVault.flatMap(vault=>MatrixStoreHelper.findByFilename(vault,
              searchPath,
              Seq("GNM_COMMISSION_ID","GNM_BEING_WRITTEN","GNM_PROJECT_ID","MXFS_FILENAME")))
        }

        maybeResults match {
          case Success(items)=>
            println(s"Got ${items.length} results:")
            items.foreach(item=>println(s"\t$item"))
          case Failure(err)=>
            println(s"Could not search vault: ${err.getMessage}")
        }

        if(maybeVault.isSuccess) maybeVault.get.dispose()
      case None=>
        println("Nothing to do!")
    }
  }
}

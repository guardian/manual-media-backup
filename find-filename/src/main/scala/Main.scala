import helpers.MatrixStoreHelper
import com.om.mxs.client.japi.MatrixStore

import scala.util.{Failure, Success}

object Main {
  def buildOptionParser = new scopt.OptionParser[Options]("find-filename") {
    head("find-filename", "1.x")
    opt[String]("vault-file").action((x,c)=>c.copy(vaultFile = x)).text(".vault file from MatrixStore Admin to provide credentials")
    opt[String]("path").action((x,c)=>c.copy(searchPath = x)).text("path to search for")
  }

  def main(args: Array[String]): Unit = {
    buildOptionParser.parse(args,Options()) match {
      case Some(parsedArgs)=>
        val maybeVault = UserInfoBuilder.fromFile(parsedArgs.vaultFile).map(userInfo=>MatrixStore.openVault(userInfo))

        val maybeResults = maybeVault.flatMap(vault=>MatrixStoreHelper.findByFilename(vault, parsedArgs.searchPath,Seq("GNM_COMMISSION_ID","GNM_BEING_WRITTEN","GNM_PROJECT_ID","MXFS_FILENAME")))

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

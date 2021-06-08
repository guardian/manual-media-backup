import com.om.mxs.client.japi.cred.Credentials
import com.om.mxs.client.japi.{MatrixStore, MatrixStoreConnection, Vault}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import scala.io.Source

case class MXSConnectionBuilder(hosts: Array[String], vaultId:String, accessKeyId:String, accessKeySecret:String) {
  def build() = Try {
    val credentials = Credentials.newAccessKeyCredentials(accessKeyId, accessKeySecret)

    val conn = MatrixStoreConnection.builder().withHosts(hosts).build()
    MatrixStore.builder()
      .withConnection(conn)
      .withCredentials(credentials)
      .build()
  }

}

object MXSConnectionBuilder {
  private val logger = LoggerFactory.getLogger(getClass)

  def withVault[T](mxs: MatrixStore, vaultId: String)(cb: (Vault) => Try[T]) = {
    Try {
      mxs.openVault(vaultId)
    } match {
      case Success(vault) =>
        val result = cb(vault)
        vault.dispose()
        result
      case Failure(err) =>
        logger.error(s"Could not establish vault connection: ${err.getMessage}", err)
        Failure(err)
    }
  }

  def fromYamlFile(filePath:String) = {
    import io.circe.syntax._
    import io.circe.generic.auto._

    val s = Source.fromFile(filePath, "UTF-8")
    val content = s.mkString
    s.close()

    io.circe.yaml.parser.parse(content).flatMap(_.as[MXSConnectionBuilder])
  }
}

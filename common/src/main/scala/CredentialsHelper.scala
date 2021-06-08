import com.om.mxs.client.japi.cred.Credentials

object CredentialsHelper {
  def fromAccessKey(keyId:String, secretKey:String) = Credentials.newAccessKeyCredentials(keyId, secretKey)
}

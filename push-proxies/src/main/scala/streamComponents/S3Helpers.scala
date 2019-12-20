package streamComponents

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{GetObjectMetadataRequest, ListObjectsV2Request}

import scala.util.Try

object S3Helpers {
  def checkFileExists(bucket:String, path:String)(implicit s3Client:AmazonS3):Try[Boolean] = Try {
    s3Client.doesObjectExist(bucket,path)
  }
}

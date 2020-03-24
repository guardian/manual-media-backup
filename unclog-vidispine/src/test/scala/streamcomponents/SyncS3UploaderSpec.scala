package streamcomponents

import java.io.File

import akka.stream.scaladsl.{FileIO, GraphDSL, RunnableGraph}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import models.S3Target
import org.specs2.mutable.Specification
import vsStreamComponents.AkkaTestkitSpecs2Support
import scala.concurrent.duration._
import scala.concurrent.Await

class SyncS3UploaderSpec extends Specification {
  "SyncS3Uploader" should {
    "upload a large file in chunks" in new AkkaTestkitSpecs2Support {
      if (sys.env.get("INTERNAL_TESTING").isDefined) {
        implicit val mat: Materializer = ActorMaterializer.create(system)

        val client = AmazonS3ClientBuilder.defaultClient()

        val sinkFact = new SyncS3Uploader(S3Target("archivehunter-test-media", "test.mp4"), client)
        val graph = GraphDSL.create(sinkFact) { implicit builder =>
          sink =>
            import akka.stream.scaladsl.GraphDSL.Implicits._
            val src = builder.add(
              FileIO.fromPath(
                new File("/Downloads/Pictures/HTC_DEVICE_MEDIA//GALLERY/storage/emulated/0/dcim/100media/video0002.mp4")
                  .toPath))

            src ~> sink
            ClosedShape
        }

        val result = Await.result(RunnableGraph.fromGraph(graph).run(), 30 seconds)
        result.getBucketName mustEqual "archivehunter-test-media"

      }
    }
  }
}

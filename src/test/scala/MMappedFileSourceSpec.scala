import java.io.{File, FileInputStream, FileOutputStream}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph}
import org.specs2.mutable.{BeforeAfter, Specification}
import org.specs2.mock.Mockito
import streamcomponents.{ChecksumSink, MMappedFileSource}
import org.apache.commons.io.IOUtils

import scala.concurrent.Await
import scala.concurrent.duration._

class MMappedFileSourceSpec extends Specification with BeforeAfter with Mockito {
  override def before = {
    val dir = new File("testfiles")
    if(!dir.exists()) dir.mkdirs()

    val file = new File("testfiles/large-test-file.mp4")
    if(!file.exists()) {
      val input = new File("/dev/urandom")

      val inputStream = new FileInputStream(input)
      val outputStream = new FileOutputStream(file)

      try {
        IOUtils.copyLarge(inputStream, outputStream, 0L, 100 * 1024 * 1024L)
      } finally {
        inputStream.close()
        outputStream.close()
      }
    }
  }

  override def after = {
    val file = new File("testfiles/large-test-file.mp4")

    if(file.exists()) file.delete();
  }

  "MMappedFileSource" should {
    "correctly read in a large file of data" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)
      val file = new File("testfiles/large-test-file.mp4")
      val sinkFactory = new ChecksumSink()

      val graph = GraphDSL.create(sinkFactory) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(new MMappedFileSource(file).async)
        src~>sink
        ClosedShape
      }

      val result = Await.result(RunnableGraph.fromGraph(graph).run(), 30 seconds)

      result must beSome
    }
  }
}

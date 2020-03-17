package vsStreamComponents

import akka.stream.{ActorMaterializer, Materializer}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import vidispine.VSCommunicator
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class VSDeleteShapeAndOrFileSpec extends Specification with Mockito {
  "VSDeleteShapeAndOrFile" should {
    "make concurrent shape and file deletion requests" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      implicit val mockedCommunicator = mock[VSCommunicator]
      mockedCommunicator.request(any,any,any,any,any,any)(any,any) returns Future(Right("mock result"))


    }
  }
}

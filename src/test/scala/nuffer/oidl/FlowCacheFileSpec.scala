package nuffer.oidl

import java.nio.file.Files

import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.StreamSpec
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class FlowCacheFileSpec extends StreamSpec {
  val settings = ActorMaterializerSettings(system)
    .withInputBuffer(initialSize = 1, maxSize = 1)

  implicit val materializer = ActorMaterializer(settings)

  "CacheFile" must {
    "pass elements through normally" in {
      val tempFilePath = Files.createTempFile("FlowBufferToFileSpec", "tmp")
      val future: Future[Vector[Byte]] = Source.repeat(ByteString("a"))
        .take(100)
        .via(CacheFile.flow(tempFilePath, expectedSize = 100))
        .grouped(101)
        .map(x => x.flatten.toVector)
        .runWith(Sink.head)
      Await.result(future, 3 seconds) should be(Vector.fill(100)('a'.toByte))
      val fileContents = Files.readAllBytes(tempFilePath)
      fileContents should be((1 to 100).map(_ => 'a').toArray)
      Files.delete(tempFilePath)

    }
  }
}

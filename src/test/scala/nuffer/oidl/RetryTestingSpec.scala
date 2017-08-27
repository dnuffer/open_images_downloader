package nuffer.oidl

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.stream.scaladsl.Source
import akka.stream.testkit.StreamSpec
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}

import scala.concurrent.Await
import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps
import scala.util.Success

class RetryTestingSpec extends StreamSpec {
  val settings = ActorMaterializerSettings(system)
    .withInputBuffer(initialSize = 1, maxSize = 1)

  implicit val materializer = ActorMaterializer(settings)

  override def expectedTestDuration: FiniteDuration = 10 minutes

  "http stream failure" must {
    "should retry" in {
      Source.single((HttpRequest(uri = "http://router/"), "http://router/"))
        .via(Http().superPool())
        .runForeach({
          case (Success(httpResponse), url) =>
            println(httpResponse)
            Await.result(httpResponse.discardEntityBytes().future(), Duration.Inf)
          case _ => assert(false)
        })

      // hook up a looping flow that re-inserts failures into the stream to get retried.
    }
  }
}

package nuffer.oidl

import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.StreamSpec
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.language.postfixOps

class FlowIfThenElse extends StreamSpec {

  val settings = ActorMaterializerSettings(system)
    .withInputBuffer(initialSize = 1, maxSize = 1)

  implicit val materializer = ActorMaterializer(settings)

  override def expectedTestDuration: FiniteDuration = 10 seconds

  "FlowIfThenElse" must {
    "pass items through flows" in {
      Await.result({
        Source[Boolean](List(false, true))
          .via(IfThenElse.flow(x => x,
            Flow.fromFunction(_ => "true flow"),
            Flow.fromFunction(_ => "false flow")
          ))
          .runWith(Sink.seq)
      }, Duration.Inf) shouldEqual Seq("false flow", "true flow")
    }
  }
}

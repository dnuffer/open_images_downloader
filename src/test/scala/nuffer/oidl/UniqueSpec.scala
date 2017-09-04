package nuffer.oidl

import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.StreamSpec
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}

import scala.collection.immutable._
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.language.postfixOps

class UniqueSpec extends StreamSpec {
  val settings = ActorMaterializerSettings(system)
    .withInputBuffer(initialSize = 1, maxSize = 1)

  implicit val materializer = ActorMaterializer(settings)

  override def expectedTestDuration: FiniteDuration = 10 minutes

  "Unique.flow()" must {
    "output nothing on empty input" in {
      Await.result(Source.empty.via(Unique.flow()).runWith(Sink.seq),
        Duration.Inf) shouldEqual Seq()
    }

    "output a single element" in {
      Await.result(Source.single(1).via(Unique.flow()).runWith(Sink.seq),
        Duration.Inf) shouldEqual Seq(1)
    }

    "output two different elements" in {
      Await.result(Source(Seq(1, 2)).via(Unique.flow()).runWith(Sink.seq),
        Duration.Inf) shouldEqual Seq(1, 2)
    }

    "output one from two same elements" in {
      Await.result(Source(Seq(1, 1)).via(Unique.flow()).runWith(Sink.seq),
              Duration.Inf) shouldEqual Seq(1)
    }

    "output three distinct elements" in {
      Await.result(Source(Seq(1, 2, 3)).via(Unique.flow()).runWith(Sink.seq),
                    Duration.Inf) shouldEqual Seq(1, 2, 3)

    }
    "output three distinct elements repeat" in {
      Await.result(Source(Seq(1, 2, 1)).via(Unique.flow()).runWith(Sink.seq),
                    Duration.Inf) shouldEqual Seq(1, 2, 1)

    }
    "output two of three elements" in {
      Await.result(Source(Seq(1, 2, 2)).via(Unique.flow()).runWith(Sink.seq),
                    Duration.Inf) shouldEqual Seq(1, 2)

    }
    "output two of three elements 2" in {
      Await.result(Source(Seq(1, 1, 2)).via(Unique.flow()).runWith(Sink.seq),
                    Duration.Inf) shouldEqual Seq(1, 2)

    }
    "output one of three elements" in {
      Await.result(Source(Seq(1, 1, 1)).via(Unique.flow()).runWith(Sink.seq),
                    Duration.Inf) shouldEqual Seq(1)

    }
  }
}

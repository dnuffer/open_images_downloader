package nuffer.oidl

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, SinkQueueWithCancel, Source}
import akka.stream.testkit._
import akka.util.ByteString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.Success

class FlowInputSwitcherSpec extends StreamSpec {

  val settings = ActorMaterializerSettings(system)
    .withInputBuffer(initialSize = 1, maxSize = 1)

  implicit val materializer = ActorMaterializer(settings)

  "InputSwitcher" must {
    "pass through in0 if never switched" in {
      val in0: Source[ByteString, NotUsed] = Source(List(ByteString(0)))
      val in1: Source[ByteString, NotUsed] = Source(List(ByteString(1)))

      val s = InputSwitcher.source(in0, in1, () => Future(false), _ => None)
      val out: Sink[ByteString, SinkQueueWithCancel[ByteString]] = Sink.queue[ByteString]().withAttributes(Attributes.inputBuffer(1, 1))

      val rg: RunnableGraph[SinkQueueWithCancel[ByteString]] = s.toMat(out)(Keep.right)
      val queue: SinkQueueWithCancel[ByteString] = rg.run()
      Await.result(queue.pull(), 3 seconds) should be(Some(ByteString(0)))
      Await.result(queue.pull(), 3 seconds) should be(None)
    }

    "pass through in0 if switched" in {
      val in0: Source[ByteString, NotUsed] = Source(List(ByteString(0)))
      val in1: Source[ByteString, NotUsed] = Source(List(ByteString(1)))

      val p = Promise[Boolean]()
      p.complete(Success(true))
      val s = InputSwitcher.source(in0, in1, () => p.future, _ => None)
      val out: Sink[ByteString, SinkQueueWithCancel[ByteString]] = Sink.queue[ByteString]().withAttributes(Attributes.inputBuffer(1, 1))

      val rg: RunnableGraph[SinkQueueWithCancel[ByteString]] = s.toMat(out)(Keep.right)
      val queue: SinkQueueWithCancel[ByteString] = rg.run()
      Await.result(queue.pull(), 3 seconds) should be(Some(ByteString(0)))
      Await.result(queue.pull(), 3 seconds) should be(None)

    }
  }
}

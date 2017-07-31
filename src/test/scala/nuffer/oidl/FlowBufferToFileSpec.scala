/**
  * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
  */
package nuffer.oidl

import java.nio.file.Files

import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit._
import akka.util.ByteString

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class FlowBufferToFileSpec extends StreamSpec {

  val settings = ActorMaterializerSettings(system)
    .withInputBuffer(initialSize = 1, maxSize = 1)

  implicit val materializer = ActorMaterializer(settings)

  "BufferToFile" must {

    "pass elements through normally" in {
      val tempFilePath = Files.createTempFile("FlowBufferToFileSpec", "tmp")
      val future: Future[Vector[Byte]] = Source.repeat(ByteString("a"))
        .take(100)
        .via(BufferToFile(tempFilePath))
        .grouped(101)
        .map(x => x.flatten.toVector)
        .runWith(Sink.head)
      Await.result(future, 3.seconds) should be(Vector.fill(100)('a'.toByte))
      val fileContents = Files.readAllBytes(tempFilePath)
      fileContents should be((1 to 100).map(_ => 'a').toArray)
      Files.delete(tempFilePath)
    }

    "pass elements through slow source" in {
      val tempFilePath = Files.createTempFile("FlowBufferToFileSpec", "tmp")
      val future: Future[Vector[Byte]] = Source.repeat(ByteString("a"))
        .throttle(1, 10 millis, 1, ThrottleMode.Shaping)
        .take(100)
        .via(BufferToFile(tempFilePath))
        .grouped(101)
        .map(x => x.flatten.toVector)
        .runWith(Sink.head)
      Await.result(future, 3.seconds) should be(Vector.fill(100)('a'.toByte))
      val fileContents = Files.readAllBytes(tempFilePath)
      fileContents should be((1 to 100).map(_ => 'a').toArray)
      Files.delete(tempFilePath)
    }

    "pass elements through slow sink" in {
      val tempFilePath = Files.createTempFile("FlowBufferToFileSpec", "tmp")
      val future: Future[Vector[Byte]] = Source.repeat(ByteString("a"))
        .take(100)
        .via(BufferToFile(tempFilePath))
        .throttle(1, 10 millis, 1, ThrottleMode.Shaping)
        .grouped(101)
        .map(x => x.flatten.toVector)
        .runWith(Sink.head)
      Await.result(future, 3.seconds) should be(Vector.fill(100)('a'.toByte))
      val fileContents = Files.readAllBytes(tempFilePath)
      fileContents should be((1 to 100).map(_ => 'a').toArray)
      Files.delete(tempFilePath)
    }

    "accept elements that fit in the buffer while downstream is silent" in {
      val publisher = TestPublisher.probe[ByteString]()
      val subscriber = TestSubscriber.manualProbe[ByteString]()
      val tempFilePath = Files.createTempFile("FlowBufferToFileSpec", "tmp")

      Source.fromPublisher(publisher)
        .via(BufferToFile(tempFilePath))
        .to(Sink.fromSubscriber(subscriber))
        .run()
      val sub = subscriber.expectSubscription()

      // Fill up buffer
      for (i ← 1 to 100) publisher.sendNext(ByteString(i))

      // drain
      sub.request(1)
      subscriber.expectNext(ByteString((1 to 100).map(_.toByte).toArray))
      sub.cancel()
    }
  }
}
package nuffer.oidl

import java.security.MessageDigest

import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.stage._
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.concurrent.Future
import scala.util.Success

/**
  * The DigestCalculator transforms/digests a stream of akka.util.ByteString to a
  * DigestResult according to a given Algorithm
  */
object DigestCalculator {

  def apply(algorithm: Algorithm): Flow[ByteString, DigestResult, NotUsed] =
    Flow.fromGraph[ByteString, DigestResult, NotUsed](new DigestCalculator(algorithm))

  def flow(algorithm: Algorithm): Flow[ByteString, DigestResult, NotUsed] =
    apply(algorithm)

  def asMat(algorithm: Algorithm): Flow[ByteString, ByteString, Future[DigestResult]] =
    Flow[ByteString].alsoToMat(sink(algorithm))(Keep.right)

  /**
    * Returns the String encoded as Hex representation of the digested stream of [[akka.util.ByteString]]
    */
  def hexString(algorithm: Algorithm): Flow[ByteString, String, NotUsed] =
    flow(algorithm).map(res => res.messageDigest.toArray.map("%02x".format(_)).mkString).fold("")(_ + _)

  def sink(algorithm: Algorithm): Sink[ByteString, Future[DigestResult]] =
    flow(algorithm).toMat(Sink.head)(Keep.right)

  def source(algorithm: Algorithm, text: String): Source[String, NotUsed] =
    Source.single(ByteString(text)).via(hexString(algorithm))
}

class DigestCalculator(algorithm: Algorithm) extends GraphStage[FlowShape[ByteString, DigestResult]] {
  val in: Inlet[ByteString] = Inlet("DigestCalculator.in")
  val out: Outlet[DigestResult] = Outlet("DigestCalculator.out")
  override val shape: FlowShape[ByteString, DigestResult] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    val digest: MessageDigest = java.security.MessageDigest.getInstance(algorithm.toString)

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
    })

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val chunk = grab(in)
        digest.update(chunk.toArray)
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        emit(out, DigestResult(ByteString(digest.digest()), Success(Done)))
        completeStage()
      }
    })
  }
}

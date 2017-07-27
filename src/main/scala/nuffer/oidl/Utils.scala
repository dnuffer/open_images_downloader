package nuffer.oidl

import java.util.Base64

import akka.stream.SinkShape
import akka.stream.scaladsl.{Broadcast, GraphDSL, Sink}
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

object Utils {
  def decodeBase64Md5(expectedMd5: String): ByteString = {
    val decoder = Base64.getDecoder
    val decodedMd5 = ByteString(decoder.decode(expectedMd5))
    decodedMd5
  }

  def hexify(byteString: ByteString): String = {
    byteString.map("%02x".format(_)).mkString
  }

  def broadcastToSinksSingleFuture[InT, Out1T, Out2T](sink1: Sink[InT, Future[Out1T]], sink2: Sink[InT, Future[Out2T]])(implicit ec: ExecutionContext): Sink[InT, Future[(Out1T, Out2T)]] = {
    broadcastToSinks(sink1, sink2).mapMaterializedValue(combineFutures)
  }

  def combineFutures[Out2T, Out1T, InT](implicit ec: ExecutionContext): ((Future[Out1T], Future[Out2T])) => Future[(Out1T, Out2T)] = {
    tupleOfFutures => for {v1 <- tupleOfFutures._1; v2 <- tupleOfFutures._2} yield (v1, v2)
  }

  def broadcastToSinks[InT, Out1T, Out2T](sink1: Sink[InT, Out1T], sink2: Sink[InT, Out2T]): Sink[InT, (Out1T, Out2T)] = {
    Sink.fromGraph(GraphDSL.create(sink1, sink2)((_, _)) {
      implicit builder =>
        (sink1, sink2) => {
          import akka.stream.scaladsl.GraphDSL.Implicits._
          val streamFan = builder.add(Broadcast[InT](2))

          streamFan.out(0) ~> sink1
          streamFan.out(1) ~> sink2

          SinkShape(streamFan.in)
        }
    })
  }
}

package nuffer.oidl

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.{FlowShape, Graph}


object Unique {
  /**
    * Remove consecutive duplicates in the stream
    * Inspired by https://github.com/akka/akka/issues/19395#issuecomment-318913370
    */
  def flow[T](): Graph[FlowShape[T, T], NotUsed] = Flow[T]
    .sliding(2, 1)
    .statefulMapConcat({
      var first = true
      () => {
        case prev +: current +: _ =>
          if (first) {
            first = false
            if (prev == current) List(prev)
            else List(prev, current)
          } else {
            if (prev == current) Nil
            else List(current)
          }
        case current +: _ =>
          List(current)
      }
    })
}

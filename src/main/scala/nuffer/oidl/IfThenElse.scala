package nuffer.oidl

import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Merge, Partition}
import akka.stream.{FlowShape, Graph}

object IfThenElse {
  def flow[T, U, Mat](test: (T) => Boolean,
                      trueFlow: Flow[T, U, Mat],
                      falseFlow: Flow[T, U, Mat]): Graph[FlowShape[T, U], Mat] =
    Flow.fromGraph(GraphDSL.create(trueFlow, falseFlow)(Keep.left) {
      implicit builder: GraphDSL.Builder[Mat] =>
        (trueFlowShape: FlowShape[T, U], falseFlowShape: FlowShape[T, U]) =>
          import GraphDSL.Implicits._

          val partition = builder.add(Partition[T](2, x => if (test(x)) 0 else 1))
          partition ~> trueFlowShape
          partition ~> falseFlowShape
          val merge = builder.add(Merge[U](2))
          merge <~ trueFlowShape
          merge <~ falseFlowShape
          FlowShape(partition.in, merge.out)
    })

}

package nuffer.oidl

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Source}
import akka.stream.stage._

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Success, Try}

object InputSwitcher {
  def fromInputs[T](in0: Source[T, _],
                    in1: Source[T, _],
                    switchFactory: () => Future[Boolean],
                    seekSecondInputCB: (Long) => Unit
                   ): Graph[SourceShape[T], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val switcher = b.add(InputSwitcher[T](switchFactory, seekSecondInputCB))
      in0 ~> switcher.in(0)
      in1 ~> switcher.in(1)
      SourceShape(switcher.out)
    }

  def source[T](in0: Source[T, _],
                in1: Source[T, _],
                switchFactory: () => Future[Boolean],
                seekSecondInputCB: (Long) => Unit
               ): Source[T, _] =
    Source.fromGraph(fromInputs(in0, in1, switchFactory, seekSecondInputCB))

  def flow[T](in1: Source[T, _],
              switchFactory: () => Future[Boolean],
              seekSecondInputCB: (Long) => Unit
             ): Flow[T, T, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val switcher = b.add(InputSwitcher[T](switchFactory, seekSecondInputCB))
      in1 ~> switcher.in(1)
      FlowShape(switcher.in(0), switcher.out)
    })
}

case class InputSwitcher[T](switchFactory: () => Future[Boolean],
                            seekSecondInput: (Long) => Unit
                           ) extends GraphStage[UniformFanInShape[T, T]] {
  val in: immutable.IndexedSeq[Inlet[T]] = Vector.tabulate(2)(i ⇒ Inlet[T]("InputSwitcher.in" + i))
  val out: Outlet[T] = Outlet[T]("InputSwitcher.out")
  override val shape: UniformFanInShape[T, T] = UniformFanInShape(out, in: _*)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {

    private var firstInputNumRead: Long = 0
    private var switch: Future[Boolean] = _
    private var switched: Boolean = false

    private def currentIn(): Inlet[T] = in(if (switched) 1 else 0)

    // need to use this callback to avoid threading race conditions with the switch future callback.
    val switchCompleteCB: AsyncCallback[Try[Boolean]] = getAsyncCallback[Try[Boolean]]({
      case Success(b) =>
        println("swithCompleteCB: " + b)
        switched = b
        if (b) {
          seekSecondInput(firstInputNumRead)
          cancel(in(0))
          if (isAvailable(out) && !hasBeenPulled(in(1))) {
            pull(in(1))
          }
        }
      case _ =>
        switched = false
    })
    private val invokeSwitchCompleteCB: Try[Boolean] => Unit = switchCompleteCB.invoke

    override def preStart(): Unit = {
      switch = switchFactory()
      switch.onComplete(invokeSwitchCompleteCB)(akka.dispatch.ExecutionContexts.global())
      //      tryPull(currentIn())
    }

    override def onPull(): Unit = {
      pull(currentIn())
    }

    in.foreach { i =>
      setHandler(i, new InHandler {
        override def onPush(): Unit = {
          if (i == currentIn()) {
            push(out, grab(currentIn()))
            if (i == in(0)) {
              firstInputNumRead += 1
            }
          }
        }

        override def onUpstreamFinish(): Unit = {
          if (i == currentIn()) {
            super.onUpstreamFinish()
          }
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          if (i == currentIn()) {
            super.onUpstreamFailure(ex)
          }
        }
      })
    }

    setHandler(out, this)
  }

  override def toString = "InputSwitcher"

}

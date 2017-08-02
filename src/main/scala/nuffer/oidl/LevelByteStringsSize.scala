package nuffer.oidl

import akka.stream.Attributes
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString

final case class LevelByteStringsSize(chunkSize: Int = 8192) extends SimpleLinearGraphStage[ByteString] {
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    var buffer: ByteString = _

    override def preStart(): Unit = {
      buffer = ByteString()
      pull(in)
    }

    private def pushChunkToOut(): Unit = {
      val (chunk, newBuffer) = buffer.splitAt(chunkSize)
      push(out, chunk)
      buffer = newBuffer
    }

    override def onPush(): Unit = {
      val elem = grab(in)
      buffer ++= elem
      // If out is available, then it has been pulled but no dequeued element has been delivered.
      // Push the current element to out, then pull.
      if (isAvailable(out) && buffer.length >= chunkSize) {
        pushChunkToOut()
      }
      if (buffer.length < chunkSize) {
        pull(in)
      }
    }

    override def onPull(): Unit = {
      if (buffer.nonEmpty) {
        pushChunkToOut()
      }
      if (isClosed(in)) {
        if (buffer.isEmpty) {
          completeStage()
        }
      } else if (!hasBeenPulled(in)) {
        if (buffer.length < chunkSize) {
          pull(in)
        }
      }
    }

    override def onUpstreamFinish(): Unit = {
      if (buffer.isEmpty) {
        completeStage()
      } else if (isAvailable(out)) {
        pushChunkToOut()
      }
    }

    setHandlers(in, out, this)
  }
}

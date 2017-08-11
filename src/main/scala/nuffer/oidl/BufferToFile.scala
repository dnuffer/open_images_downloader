package nuffer.oidl

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}

import akka.stream.Attributes
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString

final case class BufferToFile(filename: Path, chunkSize: Int = 8192) extends SimpleLinearGraphStage[ByteString] {

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    private var fileInputChan: FileChannel = _
    private var fileOutputChan: FileChannel = _
    val readByteBuffer = ByteBuffer.allocate(chunkSize)

    override def preStart(): Unit = {
      fileOutputChan = FileChannel.open(filename, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
      fileInputChan = FileChannel.open(filename, StandardOpenOption.READ)

      pull(in)
    }

    override def onPush(): Unit = {
      val elem: ByteString = grab(in)
      val written = fileOutputChan.write(elem.asByteBuffers.toArray)
      // file writes write all bytes unless something failed (see man write(2))
      if (written != elem.size) {
        throw new IOException("failed to write complete buffer")
      }
      // If out is available, then it has been pulled but no dequeued element has been delivered.
      // It means the buffer at this moment is definitely empty,
      // so we push the current element to out, then pull. And skip forward fileInputChan
      if (isAvailable(out)) {
        fileInputChan.position(fileInputChan.position() + elem.size)
        push(out, elem)
      }
      pull(in)
    }

    override def onPull(): Unit = {

      // read returns -1 when it has reached EOF, but will succeed when more data has been written to the file.
      val bytesRead = fileInputChan.read(readByteBuffer)
      if (bytesRead > 0) {
        readByteBuffer.flip()
        push(out, ByteString.fromByteBuffer(readByteBuffer))
        readByteBuffer.clear()
      }
      if (isClosed(in)) {
        if (bytesRead <= 0) {
          completeStage()
        }
      } else if (!hasBeenPulled(in)) {
        pull(in)
      }
    }

    override def onUpstreamFinish(): Unit = {
      if (fileInputChan.position() == fileInputChan.size()) {
        completeStage()
      }
    }

    override def onUpstreamFailure(ex: Throwable): Unit = {
      super.onUpstreamFailure(ex)
      Files.deleteIfExists(filename)
    }

    setHandlers(in, out, this)
  }

}

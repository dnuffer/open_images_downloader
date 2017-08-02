package nuffer.oidl

import java.nio.file.{Files, Path}

import akka.NotUsed
import akka.stream._
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.scaladsl.{FileIO, Flow, Sink}
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString

import scala.util.{Success, Try}

object CacheFile {
  def flow(filename: Path,
            expectedSize: Long,
            expectedMd5: Option[ByteString] = None,
            saveFile: Boolean = true,
            chunkSize: Int = 8192): Graph[FlowShape[ByteString, ByteString], NotUsed] = {
    // use cases:
    //   * file exists, expectedMd5==None - use it for input
    //   * file exists, expectedMd5==Some - start async computing md5 of file, use it for input after md5 computed, before that time use input.
    //   * file doesn't exist (or wrong size), saveFile==true - save input into it, use file for buffered input (BufferToFile)
    //   * file doesn't exist (or wrong size), saveFile==false - pass input straight to output
    // if the file already exists, with expected size and md5 sum, then
    // close the input and use the file for input
    // how to asynchronously check the md5? maybe start calculating the md5 in a future and until it finishes, simply read the input.
    // then once the md5 verification is done, switch to reading from the file. Call Future.isCompleted in onPush()/onPull() to see.

    val fileSizeMatchesExpected = Try(Files.size(filename)) match {
      case Success(size) =>
        if (size == expectedSize) {
          true
        } else {
          false
        }
      case _ => false
    }

    val canImmediatelyUseFileForInput: Boolean = expectedMd5.isEmpty && fileSizeMatchesExpected

    // file exists, expectedMd5==None - use it for input
    if (canImmediatelyUseFileForInput) {
      // The input is connected to Sink.cancelled as we don't need to consume it. The output comes from the cached file.
      Flow.fromSinkAndSource(Sink.cancelled, FileIO.fromPath(filename))
//      GraphDSL.create() {
//        implicit builder =>
//          // Make a new flow. The input is connected to Sink.cancelled as we don't need to consume it.
//          // Make a new source from the file and connect it to the output.
//          val canceledSink: SinkShape[ByteString] = builder.add(Sink.cancelled)
//          val fileSource: SourceShape[ByteString] = builder.add(FileIO.fromPath(filename))
//          FlowShape(in = canceledSink.in, out = fileSource.out)
//      }
//    } else if (fileSizeMatchesExpected && expectedMd5.isDefined) {
      // maybe I can use recoverWith? it switches streams. The difficulty comes when we need to also save bytes
      // while checking the md5 of the existing file.
      // it's going to be simpler to just check the md5 first and then do the rest. But that's not async!!!!! argh!!!!

      // buffer the input
      // start the md5 calc on the file
      // zip the input & md5? The md5 would need to repeat indefinitely.
      // or use something similar to initialDelay, but it waits for the md5 calc to finish. Inner is a graph with three inputs:
      // 1. bool (md5 calc != expected)
      // 2. current data stream
      // 3. cached data stream
      // And 1 output - the data
      // to ensure that current and cached stay in sync, need to ensure that the ByteStrings are all the same size.
      // similar to Chunker http://doc.akka.io/docs/akka/2.5.3/scala/stream/stream-cookbook.html#chunking-up-a-stream-of-bytestrings-into-limited-size-bytestrings, but it also needs to combine elements that are smaller.
      // could be done in 2 steps, first group up too small ones, second chunk up too large. Make this it's own stage.
      // until the bool is read, read from current and cached in sync.
      // When the bool is read, close the stream that is no longer useful.
      // after the bool is read, read from stream 1 if true, otherwise read from stream 2.
    } else {
      new CacheFile(filename, expectedSize, expectedMd5, saveFile, chunkSize)
    }
  }
}

final case class CacheFile(filename: Path,
                           expectedSize: Long,
                           expectedMd5: Option[ByteString] = None,
                           saveFile: Boolean = true,
                           chunkSize: Int = 8192
                          ) extends SimpleLinearGraphStage[ByteString] {
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    // use cases:
    //   * file exists, expectedMd5==None - use it for input
    //   * file exists, expectedMd5==Some - start async computing md5 of file, use it for input after md5 computed, before that time use input.
    //   * file doesn't exist, saveFile==true - save input into it, use file for buffered input (BufferToFile)
    //   * file doesn't exist, saveFile==false - pass input straight to output
    // if the file already exists, with expected size and md5 sum, then
    // close the input and use the file for input
    // how to asynchronously check the md5? maybe start calculating the md5 in a future and until it finishes, simply read the input.
    // then once the md5 verification is done, switch to reading from the file. Call Future.isCompleted in onPush()/onPull() to see.
    override def onPush(): Unit = ???

    override def onPull(): Unit = ???
  }
}

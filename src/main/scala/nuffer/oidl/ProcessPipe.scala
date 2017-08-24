package nuffer.oidl

import java.io.InputStream

import akka.Done
import akka.stream.QueueOfferResult.{Dropped, Enqueued, QueueClosed}
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.util.ByteString

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.sys.process.BasicIO.BufferSize
import scala.util.{Failure, Success}

object ProcessPipe {
  // need an unlimited size thread pool for blocking io threads.
  implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newCachedThreadPool())

  case class ProcessFailedException(command: String, exitCode: Int, stderr: ByteString = ByteString()) extends RuntimeException

  def transferFully(inStream: InputStream, outQueue: SourceQueueWithComplete[ByteString]) {

    def asyncLoop(): Future[Done] =
      Future {
        blocking {
          try {
            val buffer = new Array[Byte](BufferSize)
            val bytesRead = inStream.read(buffer)
            if (bytesRead == -1) {
              Success(None)
            } else
              Success(Some(ByteString(buffer.take(bytesRead))))
          } catch {
            case exception: Throwable => Failure(exception)
          }
        }
      }(ec)
        .flatMap {
          case Success(Some(bytes)) => outQueue.offer(bytes)
          case Success(None) => Future(outQueue.complete()) // upstream reached EOF, so complete the out queue
          case Failure(exception) =>
            outQueue.fail(exception)
            Future.failed(exception) // upstream exception
        }
        .flatMap {
          case Enqueued => asyncLoop() // passed along data, try to read more
          case Dropped => asyncLoop() // if the downstream didn't want it... just keep going
          case QueueOfferResult.Failure(exception) => Future.failed(exception) // downstream exception
          case QueueClosed => Future.successful(Done) // downstream closed
          case _ => Future.successful(Done) // upstream reached EOF and out complete
        }

    try {
      Await.result(asyncLoop(), Duration.Inf)
    }
    finally {
      inStream.close()
    }
  }

  def throughProcessCheckForError(command: String)(implicit materializer: Materializer): Flow[ByteString, ByteString, Future[Done]] = {
    Flow.fromSinkAndSourceMat(Sink.queue[ByteString](), Source.queue[ByteString](1, OverflowStrategy.backpressure))((stdinQueue, stdoutQueue) => Future {
      blocking {
        val proc: java.lang.Process = new java.lang.ProcessBuilder(command.split("""\s+"""): _*).start()

        val stdinThread = new Thread {
          try {
            def pull(): Future[Done] = {
              stdinQueue.pull() flatMap {
                case None =>
                  proc.getOutputStream.close()
                  Future.successful(Done)
                case Some(byteString) =>
                  proc.getOutputStream.write(byteString.toArray)
                  pull()
              }
            }
            Await.result(pull(), Duration.Inf)
          } catch {
            case exception: Throwable =>
              proc.getOutputStream.close()
              stdoutQueue.fail(exception)
              proc.destroyForcibly()
          }
        }
        stdinThread.start()

        val stdoutThread = new Thread {
          transferFully(proc.getInputStream, stdoutQueue)
        }
        stdoutThread.start()

        // make stderrQueue a sink with a limit, and a queue
        val (stderrQueue: SourceQueueWithComplete[ByteString], futureStderrBytes: Future[Option[ByteString]]) =
          Source.queue[ByteString](1, OverflowStrategy.backpressure)
            .batchWeighted(1024 * 1024, _.size, Vector(_))(_ :+ _) // accumulate up to 1 MB of stderr in a Vector[ByteString]
            .take(1) // this prevents the us from accumulating more than 1 MB.
            .map(_.fold(ByteString())(_ ++ _)) // flatten the vector back to a single ByteString
            .toMat(Sink.headOption)((_, _)) // keep the first ByteString and the queue to use for stderr
            .run()
        val stderrThread = new Thread {
          transferFully(proc.getErrorStream, stderrQueue)
        }
        stderrThread.start()

        proc.waitFor()
        stdinThread.join()
        stdoutThread.join()
        stderrThread.join()
        val exitCode: Int = proc.exitValue()

        if (exitCode != 0) {
          val errStr = Await.result(futureStderrBytes, Duration.Inf).getOrElse(ByteString())
          throw ProcessFailedException(command, exitCode, errStr)
        }

        Done
      }
    }(ec))
  }
}

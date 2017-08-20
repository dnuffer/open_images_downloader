package nuffer.oidl

import java.io.{IOException, InputStream}

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

  def transferFully(in: InputStream, out: SourceQueueWithComplete[ByteString]) {

    def asyncLoop(): Future[Done] =
      Future {
        blocking {
          try {
            val buffer = new Array[Byte](BufferSize)
            val bytesRead = in.read(buffer)
            if (bytesRead == -1) {
              Success(None)
            } else
              Success(Some(ByteString(buffer.take(bytesRead))))
          } catch {
            case ex: IOException =>
              out.fail(ex)
              Failure(ex)
            case ex: Throwable => Failure(ex)
          }
        }
      }(ec)
        .flatMap {
          case Success(Some(bytes)) => out.offer(bytes)
          case Success(None) => Future(out.complete()) // upstream reached EOF, so complete the out queue
          case Failure(exception) => Future.failed(exception) // upstream exception         }
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
      in.close()
    }
  }

  def throughProcessCheckForError(command: String)(implicit materializer: Materializer): Flow[ByteString, ByteString, Future[Done]] = {
    Flow.fromSinkAndSourceMat(Sink.queue[ByteString](), Source.queue[ByteString](1, OverflowStrategy.backpressure))((iq, oq) => Future {
      blocking {

        type JProcess = java.lang.Process
        type JProcessBuilder = java.lang.ProcessBuilder
        val proc: JProcess = new JProcessBuilder(command.split("""\s+"""): _*).start()

        val stdinThread = new Thread {
          try {
            def pull(): Future[Done] = {
              iq.pull() flatMap {
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
              oq.fail(exception)
              proc.destroyForcibly()
          }
        }
        stdinThread.start()

        val stdoutThread = new Thread {
          transferFully(proc.getInputStream, oq)
        }
        stdoutThread.start()

        // make es a sink with a limit, and a queue
        val (es: SourceQueueWithComplete[ByteString], futureEsOut: Future[Option[ByteString]]) = Source.queue[ByteString](1, OverflowStrategy.backpressure)
          .batchWeighted(1024 * 1024, _.size, Vector(_))(_ :+ _) // accumulate up to 1 MB of stderr in a Vector[ByteString]
          .take(1) // this prevents the us from accumulating more than 1 MB.
          .map((vbs: Vector[ByteString]) => vbs.fold(ByteString())(_ ++ _)) // flatten the vector back to a single ByteString
          .toMat(Sink.headOption)((_, _)) // keep the first ByteString and the queue to use for stderr
          .run()
        val stderrThread = new Thread {
          transferFully(proc.getErrorStream, es)
        }
        stderrThread.start()

        proc.waitFor()
        stdinThread.join()
        stdoutThread.join()
        stderrThread.join()
        val exitCode: Int = proc.exitValue()

        if (exitCode != 0) {
          throw ProcessFailedException(command, exitCode, Await.result(futureEsOut, Duration.Inf).getOrElse(ByteString()))
        }

        Done
      }
    }(ec))
  }
}

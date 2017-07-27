package nuffer.oidl

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.HttpExt
import akka.stream.ActorMaterializer

case object StartDownload

case object EndDownload

case object InputCsvProcessingEnd

case object FatalError

class Terminator(materializer: ActorMaterializer, http: HttpExt) extends Actor with ActorLogging {
  var inFlight = 0
  var inputCsvProcessingDone = false

  def receive: PartialFunction[Any, Unit] = {
    case StartDownload =>
      inFlight += 1
    case EndDownload =>
      inFlight -= 1
      checkForTermination()
    case InputCsvProcessingEnd =>
      inputCsvProcessingDone = true
      checkForTermination()
    case FatalError =>
      terminate()
  }

  private def checkForTermination(): Unit = {
    if (inFlight == 0 && inputCsvProcessingDone) {
      terminate()
    }
  }

  private def terminate(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    log.info("shutting down system")

    log.info("http.shutdownAllConnectionPools()")
    http.shutdownAllConnectionPools().onComplete({ _ =>
      log.info("materializer.shutdown()")
      materializer.shutdown()
      log.info("context.system.terminate()")
      context.system.terminate()
    })
  }
}

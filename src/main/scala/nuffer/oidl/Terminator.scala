package nuffer.oidl

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.HttpExt
import akka.stream.ActorMaterializer

case object StartDownload

case object EndDownload

case object InputCsvProcessingEnd

class Terminator(materializer: ActorMaterializer, http: HttpExt) extends Actor with ActorLogging {
  var inFlight = 0
  var inputCsvProcessingDone = false

  def receive: PartialFunction[Any, Unit] = {
    case StartDownload =>
      inFlight += 1
    case EndDownload =>
      inFlight -= 1
      checkForTermination
    case InputCsvProcessingEnd =>
      inputCsvProcessingDone = true
      checkForTermination
  }

  private def checkForTermination: Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    if (inFlight == 0 && inputCsvProcessingDone) {
      log.info("DownloadEnd, shutting down system")

      log.info("http.shutdownAllConnectionPools()")
      http.shutdownAllConnectionPools().onComplete { _ =>
        log.info("materializer.shutdown()")
        materializer.shutdown()
        log.info("context.system.terminate()")
        context.system.terminate()
      }
    }
  }
}

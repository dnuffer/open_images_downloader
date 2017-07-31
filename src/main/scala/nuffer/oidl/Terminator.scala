package nuffer.oidl

import akka.actor.{Actor, ActorLogging, ActorSystem}
import akka.http.scaladsl.Http

case object StartDownload

case object EndDownload

case object InputCsvProcessingEnd

case object FatalError

class Terminator() extends Actor with ActorLogging {
  final implicit val system: ActorSystem = context.system

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

    // if we want to avoid the false ERROR log messages at shutdown "Outgoing request stream error (akka.stream.AbruptTerminationException)"
    // we can wait for a while until the idle timeouts pass. shutdownAllConnectionPools() doesn't seem to wait properly.
//    val connectionPoolIdleTimeout: Long = context.system.settings.config.getDuration("akka.http.host-connection-pool.idle-timeout", TimeUnit.MILLISECONDS)
//    val connectionPoolClientIdleTimeout: Long = context.system.settings.config.getDuration("akka.http.host-connection-pool.client.idle-timeout", TimeUnit.MILLISECONDS)
//    log.info("Waiting for idle timeouts: {} + {} = {}", connectionPoolIdleTimeout, connectionPoolClientIdleTimeout, connectionPoolIdleTimeout + connectionPoolClientIdleTimeout)
//    Thread.sleep(connectionPoolIdleTimeout + connectionPoolClientIdleTimeout)
    log.info("http.shutdownAllConnectionPools()")
    Http().shutdownAllConnectionPools().onComplete({ _ =>
//      log.info("materializer.shutdown()")
//      materializer.shutdown()
      log.info("context.system.terminate()")
      context.system.terminate()
    })
  }
}

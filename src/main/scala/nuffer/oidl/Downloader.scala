package nuffer.oidl

import java.nio.file.{Files, Path}

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.ContentType.Binary
import akka.http.scaladsl.model.StatusCodes.Redirection
import akka.http.scaladsl.model._
import akka.stream._
import akka.stream.scaladsl.{FileIO, Sink}
import akka.util.ByteString
import nuffer.oidl.Utils._

import scala.concurrent.Future
import scala.util.{Failure, Success}

case class DownloadUrlToFile(url: String, filePath: Path, expectedSize: Long, expectedMd5: String, checkMd5IfExists: Boolean)

class Downloader(terminatorActor: ActorRef, actorMaterializer: ActorMaterializer, http: HttpExt) extends Actor
  with ActorLogging {

  import context.dispatcher

  final implicit val materializer: ActorMaterializer = actorMaterializer

  override def receive: Receive = {
    // server replied with a 200 OK
    case (Success(resp@HttpResponse(StatusCodes.OK, headers, entity1, _)),
          DownloadUrlToFile(url: String, filePath: Path, expectedSize: Long, expectedMd5: String, checkMd5IfExists: Boolean)) =>
      log.info("{} OK. Content-Type: {}", url, entity1.contentType)
      val entity = entity1.withSizeLimit(Long.MaxValue)
      entity.contentType match {
        case Binary(MediaTypes.`image/jpeg`) =>
          entity.contentLengthOption match {
            case Some(serverSize) =>
              if (serverSize == expectedSize) {
                val fileSink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(filePath)
                val md5CalculatorSink: Sink[ByteString, Future[DigestResult]] = DigestCalculator.sink(Algorithm.MD5)

                val finalSink: Sink[ByteString, Future[(IOResult, DigestResult)]] = broadcastToSinksSingleFuture(fileSink, md5CalculatorSink)
                entity.dataBytes.runWith(finalSink).foreach { result =>
                  result match {
                    case (IOResult(count, Success(_)), md5DigestResult) =>
                      val decodedMd5: ByteString = decodeBase64Md5(expectedMd5)

                      if (decodedMd5 != md5DigestResult.messageDigest) {
                        log.error("{} md5 sum doesn't match expected. actual: {}, expected: {}", url, hexify(md5DigestResult.messageDigest),
                          hexify(decodedMd5))
                        val deleted = Files.deleteIfExists(filePath)
                        if (deleted) {
                          log.info("Deleted {}", filePath)
                        }
                      }
                      log.info("Request data completely read for {}. {} bytes.", url, count)
                    case (IOResult(_, Failure(error)), _) =>
                      log.error("{}: failed saving request data to file: {}", url, error)
                      val deleted = Files.deleteIfExists(filePath)
                      if (deleted) {
                        log.info("Deleted {}", filePath)
                      }
                  }
                  terminatorActor ! EndDownload
                }
              } else {
                log.error("{}: server size ({}) doesn't match expected size ({})!", url, serverSize, expectedSize)
                resp.entity.discardBytes().future().onComplete { _ =>
                  terminatorActor ! EndDownload
                }
              }
            case None =>
              log.error("{}: No Content-Length!", url)
              resp.entity.discardBytes().future().onComplete { _ =>
                terminatorActor ! EndDownload
              }
          }
        case _ =>
          log.error("{}: Content-Type != image/jpeg", url)
          resp.entity.discardBytes().future().onComplete { _ =>
            terminatorActor ! EndDownload
          }
      }


    // handle redirects
    case (Success(resp@HttpResponse(Redirection(_), _, _, _)), downloadParam: DownloadUrlToFile) =>
      resp.header[headers.Location] match {
        case Some(location) =>
          log.info("{}: location: {}", downloadParam.url, location)
          if (location.uri.path.toString().endsWith("/photo_unavailable.png") || location.uri.path.toString.endsWith("/photo_unavailable_l.png")) {
            log.info("Got a photo unavailable redirect.")
            resp.entity.discardBytes().future().onComplete { _ =>
              terminatorActor ! EndDownload
            }
          } else {
            resp.entity.discardBytes().future().onComplete { _ =>
              self ! DownloadUrlToFile(location.uri.toString(), downloadParam.filePath, downloadParam.expectedSize, downloadParam.expectedMd5,
                downloadParam.checkMd5IfExists)
            }
          }
        case None =>
          log.error("{}: Got redirect without Location header", downloadParam.url)
          resp.entity.discardBytes().future().onComplete { _ =>
            terminatorActor ! EndDownload
          }
      }

    // handle failures
    case (Success(resp@HttpResponse(code, _, _, _)), downloadParam: DownloadUrlToFile) =>
      log.error("{}: Request failed, response code: {}", downloadParam.url, code)
      resp.entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach { body =>
        log.error("{}: failure body: {}", downloadParam.url, body.utf8String)
        terminatorActor ! EndDownload
      }

    case InputCsvProcessingEnd =>
      terminatorActor ! InputCsvProcessingEnd

    case m =>
      log.error("unexpected message: {}", m)
  }
}

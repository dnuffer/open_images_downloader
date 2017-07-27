package nuffer.oidl

import java.nio.file.{Files, Path}
import java.util.Base64

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.ContentType.Binary
import akka.http.scaladsl.model.StatusCodes.Redirection
import akka.http.scaladsl.model._
import akka.stream._
import akka.stream.scaladsl.{Broadcast, FileIO, GraphDSL, Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

case class DownloadUrlToFile(url: String, filePath: Path, expectedSize: Long, expectedMd5: String, checkMd5IfExists: Boolean)

//case class HttpResponseAndDownloadUrlToFile(response: HttpResponse, downloadUrlToFile: DownloadUrlToFile)

class Downloader(terminatorActor: ActorRef, actorMaterializer: ActorMaterializer, http: HttpExt) extends Actor
  with ActorLogging {

  import akka.pattern.pipe
  import context.dispatcher

  final implicit val materializer: ActorMaterializer = actorMaterializer

  override def receive: Receive = {
//    // start the download
//    case downloadParam@DownloadUrlToFile(url: String, filePath: Path, expectedSize: Long, expectedMd5: String, checkMd5IfExists: Boolean) =>
//      val needToDownloadFut = needToDownload(filePath, expectedSize, checkMd5IfExists, expectedMd5)
//      needToDownloadFut.andThen({
//        case Success(needToDownload) =>
//          if (needToDownload) {
//            //            log.info("Starting request for {}", url)
//            http.singleRequest(HttpRequest(uri = url)).map(httpResponse => HttpResponseAndDownloadUrlToFile(httpResponse, downloadParam)).pipeTo(self)
//          } else {
//            log.info("Skipping download of {}", url)
//            terminatorActor ! EndDownload
//          }
//      })

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

  private def decodeBase64Md5(expectedMd5: String): ByteString = {
    val decoder = Base64.getDecoder
    val decodedMd5 = ByteString(decoder.decode(expectedMd5))
    decodedMd5
  }

  private def hexify(byteString: ByteString): String = {
    byteString.map("%02x".format(_)).mkString
  }

  private def needToDownload(filePath: Path, expectedSize: Long, doCheckMd5: Boolean, expectedMd5: String): Future[Boolean] = {
    Try(Files.size(filePath)) match {
      case Success(size) =>
        if (size != expectedSize) {
          Future(true)
        } else {
          // the file exists and the size matches, now we need to verify the md5
          if (doCheckMd5) {
            val fileSource: Source[ByteString, Future[IOResult]] = FileIO.fromPath(filePath)
            val md5CalculatorSink: Sink[ByteString, Future[DigestResult]] = DigestCalculator.sink(Algorithm.MD5)
            fileSource.runWith(md5CalculatorSink).map { md5DigestResult =>
              log.info("checked md5sum of {}. got {}, expected {}",
                filePath, hexify(md5DigestResult.messageDigest), hexify(decodeBase64Md5(expectedMd5)))
              md5DigestResult.messageDigest != decodeBase64Md5(expectedMd5)
            }
          } else {
            Future(false)
          }
        }

      case Failure(_) => Future(true)
    }
  }

  private def broadcastToSinksSingleFuture[InT, Out1T, Out2T](sink1: Sink[InT, Future[Out1T]],
                                                              sink2: Sink[InT, Future[Out2T]])
  : Sink[InT, Future[(Out1T, Out2T)]] = {
    broadcastToSinks(sink1, sink2).mapMaterializedValue(combineFutures)
  }

  private def combineFutures[Out2T, Out1T, InT]: ((Future[Out1T], Future[Out2T])) => Future[(Out1T, Out2T)] = {
    tupleOfFutures => for {v1 <- tupleOfFutures._1; v2 <- tupleOfFutures._2} yield (v1, v2)
  }

  private def broadcastToSinks[InT, Out1T, Out2T](sink1: Sink[InT, Out1T], sink2: Sink[InT, Out2T]): Sink[InT, (Out1T, Out2T)] = {
    Sink.fromGraph(GraphDSL.create(sink1, sink2)((_, _)) {
      implicit builder =>
        (sink1, sink2) => {
          import akka.stream.scaladsl.GraphDSL.Implicits._
          val streamFan = builder.add(Broadcast[InT](2))

          streamFan.out(0) ~> sink1
          streamFan.out(1) ~> sink2

          SinkShape(streamFan.in)
        }
    })
  }
}

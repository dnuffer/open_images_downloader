package nuffer.oidl

import java.io.InputStream
import java.nio.file.{Files, Path, Paths}

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ContentType.Binary
import akka.http.scaladsl.model.StatusCodes.Redirection
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Location
import akka.stream._
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{Broadcast, Compression, FileIO, Flow, GraphDSL, Keep, Sink, Source, StreamConverters}
import akka.util.ByteString
import akka.{Done, NotUsed}
import nuffer.oidl.Utils.{broadcastToSinksSingleFuture, decodeBase64Md5, dehexify, hexify}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

case class DownloadParams(url: String, filePath: Path, expectedSize: Long, expectedMd5: String, checkMd5IfExists: Boolean)

case class ImageProcessingState(downloadParams: DownloadParams, needToDownload: Boolean)

case class Director(implicit system: ActorSystem) {
  val log = Logging(system, this.getClass)
  final implicit val materializer: ActorMaterializer = ActorMaterializer()

  def run(): Future[Done] = {
    val checkMd5IfExists = false
    val alwaysDownload = false

    val tarSource: Source[ByteString, NotUsed] = images_2017_07_tar_gz_source
      .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Stop))
      .via(images_2017_07_tar_gz_cache_flow)
      .via(Compression.gunzip())
      .via(images_2017_07_tar_cache_flow)

    val tarArchiveEntrySource: Source[(TarArchiveInputStream, TarArchiveEntry), NotUsed] = tarArchiveEntriesFromTarFile(tarSource)

    tarArchiveEntrySource
      .via(alsoToEagerCancelGraph(createDirsForEntrySink)) // don't use .alsoTo(), use Broadcast( , eagerCancel=true) to avoid consuming the entire input stream when downstream cancels.
      .flatMapConcat(tarArchiveEntryToCsvLines)
      .take(200) // for testing purposes, limit to 100
      .alsoTo(countAndPrintNumberOfLinesPerType)
      .map(makeDownloadRequestTuple(checkMd5IfExists, _))
      .via(mapNeedToDownloadRequest(alwaysDownload))
      .via(startDownloadOrFromFile)
      .filter(keepSuccess)
      .mapAsyncUnordered(100 /* TODO: make this the same size as the number of http streams */)(processJpegBytes)
      .via(Flow.fromFunction(processMd5Result))
      .watchTermination()(makeTerminationHandler)
      .runWith(Sink.foreach(println))
  }

  private def startDownloadOrFromFile = {
    IfThenElse.flow(
      {
        case (request: HttpRequest, state: ImageProcessingState) => state.needToDownload
      },
      Flow[(HttpRequest, ImageProcessingState)]
        .via(Http().superPool())
        .mapAsyncUnordered(100 /* TODO: make this the same size as the number of http streams */)(processHttpResponse)
      ,
      Flow[(HttpRequest, ImageProcessingState)].map({
        case (request: HttpRequest, state: ImageProcessingState) =>
          Success((FileIO.fromPath(state.downloadParams.filePath), state))
      })
    )
  }

  private def processJpegBytes: (Try[(Source[ByteString, Any], ImageProcessingState)]) => Future[Try[(IOResult, DigestResult, ImageProcessingState)]] = {
    case Success((dataBytes, state@ImageProcessingState(downloadParams, _))) =>
      val tupleOfFutures = dataBytes
        .alsoToMat(resizeImageAndSaveToFile(resizedImagePath(downloadParams.filePath)))(Keep.right)
        .toMat(saveToFileIfNotPresentAndComputeMd5(downloadParams.filePath, state.needToDownload))(Keep.both)
        .run()

      // need to wait for the resize and save file to finish, so create a joined future
      val joinedFuture = for {v1 <- tupleOfFutures._1; v2 <- tupleOfFutures._2} yield (v1, v2)

      // convert it to a Try and include downloadParams.
      joinedFuture
        .map({
          case (done, (ioResult, digestResult)) => Success((ioResult, digestResult, state))
        })
        .recover({
          case exception => Failure(exception)
        })
    case Failure(exception) =>
      Future.failed(exception)
  }

  private def keepSuccess[T]: (Try[T]) => Boolean = {
    case Success(_) => true
    case _ => false
  }

  private def processHttpResponse: ((Try[HttpResponse], ImageProcessingState)) => Future[Try[(Source[ByteString, Any], ImageProcessingState)]] = {

    // server replied with a 200 OK
    case (Success(HttpResponse(StatusCodes.OK, _, entity1, _)), state@ImageProcessingState(downloadParams@DownloadParams(url: String, _: Path, expectedSize: Long, _: String, _: Boolean), _)) =>
      log.info("{} OK. Content-Type: {}", url, entity1.contentType)
      val entity = entity1.withSizeLimit(expectedSize)
      entity.contentType match {
        case Binary(MediaTypes.`image/jpeg`) =>
          entity.contentLengthOption match {
            case Some(serverSize) =>
              if (serverSize == expectedSize) {
                Future(Success(entity.dataBytes, state))
              } else {
                log.error("{}: server size ({}) doesn't match expected size ({})!", url, serverSize, expectedSize)
                handleResponseError(entity, InvalidSizeException(url, serverSize, expectedSize))
              }
            case None =>
              log.error("{}: No Content-Length!", url)
              handleResponseError(entity, NoContentLengthHeaderException(url))
          }
        case _ =>
          log.error("{}: Content-Type != image/jpeg", url)
          handleResponseError(entity, ContentTypeNotJpegException(url))
      }


    // handle redirects
    case (Success(resp@HttpResponse(Redirection(_), _, _, _)), state@ImageProcessingState(downloadParams, _)) =>
      resp.header[Location] match {
        case Some(location) =>
          log.info("{}: location: {}", downloadParams.url, location)
          if (location.uri.path.toString().endsWith("/photo_unavailable.png") || location.uri.path.toString.endsWith("/photo_unavailable_l.png")) {
            log.info("Got a photo unavailable redirect.")
          }
        case None =>
          log.error("{}: Got redirect without Location header", downloadParams.url)
      }
      handleResponseError(resp.entity, RedirectResponseException(downloadParams.url))

    // handle failures
    case (Success(resp@HttpResponse(code, _, _, _)), state@ImageProcessingState(downloadParams, _)) =>
      log.error("{}: Request failed, response code: {}", downloadParams.url, code)
      resp.entity.withSizeLimit(1024 * 1024).dataBytes.take(1024 * 1024).runFold(ByteString(""))(_ ++ _).map { body =>
        log.error("{}: failure body: {}", downloadParams.url, body.utf8String)
        Failure(RequestFailedException(downloadParams.url, body.utf8String))
      } recover {
        case _ => Failure(RequestFailedException(downloadParams.url, ""))
      }

    case (Failure(exception), state@ImageProcessingState(downloadParams, _)) =>
      log.error("{}: Request failed: {}", downloadParams.url, exception)
      Future.successful(Failure(exception))
  }

  private def handleResponseError(entity: ResponseEntity, exception: Throwable) = {
    val MTU_BYTES: Long = 1500 // since we've got at least one packet already, we can discard some without blocking, and possibly re-use the connection.
    // Sure there's the headers (~730 bytes from flickr.com) not being accounted for and transfer encoding, so this is just a hopefully-effective heuristic.
    val REDIRECT_BYTES: Long = 3213 // the not available redirect includes this many bytes. With headers, this should limit to reading 3 packets.
    entity.withSizeLimit(REDIRECT_BYTES).discardBytes().future().transform(_ => Success(Failure(exception)))
  }

  private def processMd5Result: Try[(IOResult, DigestResult, ImageProcessingState)] => Try[ImageProcessingState] = {
    case Success((IOResult(count, Success(_)), md5DigestResult: DigestResult, state@ImageProcessingState(downloadParams, _))) =>
      val decodedMd5: ByteString = decodeBase64Md5(downloadParams.expectedMd5)

      if (decodedMd5 != md5DigestResult.messageDigest) {
        log.error("{} md5 sum doesn't match expected. actual: {}, expected: {}", downloadParams.url, hexify(md5DigestResult.messageDigest),
          hexify(decodedMd5))
        val deleted = Files.deleteIfExists(downloadParams.filePath)
        if (deleted) {
          log.info("Deleted {}", downloadParams.filePath)
        }
        Failure(MD5SumMismatch(downloadParams))
      } else {
        log.info("Request data completely read for {}. {} bytes.", downloadParams.url, count)
        Success(state)
      }
    case Success((IOResult(_, Failure(error)), _, state@ImageProcessingState(downloadParams, _))) =>
      log.error("{}: failed saving request data to file: {}", downloadParams.url, error)
      val deleted = Files.deleteIfExists(downloadParams.filePath)
      if (deleted) {
        log.info("Deleted {}", downloadParams.filePath)
      }
      Failure(error)
    case Failure(exception) =>
      log.error("Download failure: {}", exception)
      Failure(exception)
  }

  def fileExistsAndHasGreaterThanZeroSize(filePath: Path): Boolean =
    try {
      Files.size(filePath) > 0
    } catch {
      case _: Throwable => false
    }

  private def resizeImageAndSaveToFile(filePath: Path): Sink[ByteString, Future[Done]] = {
    if (fileExistsAndHasGreaterThanZeroSize(filePath)) {
      Sink.ignore
    } else {
      ImageResize.resizeJpgFlow(299)
        .watchTermination()({
          case (mat, eventualDone: Future[Done]) =>
            eventualDone.onComplete({
              case Failure(error) =>
                log.error("Resize Failed: {}", error)
                System.err.println("Resize Failed: " + error)
              case Success(_) =>
                log.info("done resizing")
            })
            mat
        })
        .to(FileIO.toPath(filePath))
    }
  }

  private def saveToFileIfNotPresentAndComputeMd5(filePath: Path, needToDownload: Boolean): Sink[ByteString, Future[(IOResult, DigestResult)]] = {
    if (needToDownload) {
      broadcastToSinksSingleFuture(FileIO.toPath(filePath), md5Sink)
    } else {
      md5Sink.mapMaterializedValue(_.map((IOResult(Files.size(filePath), Success(Done)), _)))
    }
  }

  private def resizedImagePath(filePath: Path): Path = {
    filePath.getParent.resolveSibling("images-resized").resolve(filePath.getFileName)
  }

  private def md5Sink: Sink[ByteString, Future[DigestResult]] = {
    DigestCalculator.sink(Algorithm.MD5)
  }

  def alsoToEagerCancelGraph[Out, M](that: Graph[SinkShape[Out], M]): Graph[FlowShape[Out@uncheckedVariance, Out], M] =
    GraphDSL.create(that) { implicit b ⇒
      r ⇒
        import GraphDSL.Implicits._
        val bcast = b.add(Broadcast[Out](2, eagerCancel = true))
        bcast.out(1) ~> r
        FlowShape(bcast.in, bcast.out(0))
    }

  private def makeTerminationHandler: ((NotUsed, Future[Done]) => Unit) = {

    case (_, eventualDone: Future[Done]) =>
      eventualDone.onComplete({
        case Failure(error) =>
          log.error("Graph termination handler. Failed: {}", error)
          System.err.println("\nGraph termination handler. Failed: " + error)
        case Success(_) =>
          log.info("done processing csv")
      })

  }

  private def filterNeedToDownload(alwaysDownload: Boolean): Flow[(HttpRequest, DownloadParams), (HttpRequest, ImageProcessingState), NotUsed] =
    mapNeedToDownloadRequest(alwaysDownload)
      .filter(_._2.needToDownload)

  private def mapNeedToDownloadRequest(alwaysDownload: Boolean): Flow[(HttpRequest, DownloadParams), (HttpRequest, ImageProcessingState), NotUsed] = {
    Flow[(HttpRequest, DownloadParams)]
      .mapAsyncUnordered(Runtime.getRuntime.availableProcessors() * 2)(needToDownloadRequest(alwaysDownload))
  }

  private def needToDownloadRequest(alwaysDownload: Boolean): ((HttpRequest, DownloadParams)) => Future[(HttpRequest, ImageProcessingState)] = {
    case downloadRequestTuple@(downloadRequest: HttpRequest, downloadUrlToFile: DownloadParams) =>
      for {
        nd <- needToDownload(downloadUrlToFile, alwaysDownload)
      } yield (downloadRequestTuple._1, ImageProcessingState(downloadRequestTuple._2, nd))
  }

  private def makeDownloadRequestTuple(checkMd5IfExists: Boolean, line: Map[String, ByteString]): (HttpRequest, DownloadParams) = {
    (makeRequest(line), makeDownloadUrlToFile(line, outputDir(line), checkMd5IfExists))
  }

  private def countAndPrintNumberOfLinesPerType: Sink[Map[String, ByteString], Unit] = {
    Sink.fold(Map[String, Int]().withDefaultValue(0))({
      (map: Map[String, Int], csvLine: Map[String, ByteString]) => map.updated(csvLine("Subset").utf8String, map(csvLine("Subset").utf8String) + 1)
    }).mapMaterializedValue(futureMap => futureMap.onComplete({
      case Success(m) => log.info("number of lines per type: {}", m)
      case Failure(e) => log.error("failure: {}", e)
    }))
  }

  private def tarArchiveEntryToCsvLines(tarArchiveEntry: (TarArchiveInputStream, TarArchiveEntry)): Source[Map[String, ByteString], Future[IOResult]] = tarArchiveEntry match {
    case (tarArchiveInputStream, tarEntry) =>
      StreamConverters.fromInputStream(() => TarEntryInputStream(tarArchiveInputStream))
        .via(cacheFileForTarEntry(tarEntry))
        .via(CsvParsing.lineScanner(CsvParsing.Comma, CsvParsing.DoubleQuote, '\0'))
        .via(CsvToMap.toMap())
  }

  private def createDirsForEntrySink: Sink[(TarArchiveInputStream, TarArchiveEntry), Future[Done]] = {
    Sink.foreach[(TarArchiveInputStream, TarArchiveEntry)]({
      case (tarArchiveInputStream, tarEntry) =>
        log.info("tarEntry.name: {}", tarEntry.getName)
        val parentDir = Paths.get(tarEntry.getName).getParent
        parentDir.toFile.mkdirs()
        val imagesOriginalDir = parentDir.resolve("images-original")
        imagesOriginalDir.toFile.mkdirs()
        val imagesResizedDir = parentDir.resolve("images-resized")
        imagesResizedDir.toFile.mkdirs()
    })
  }

  private def cacheFileForTarEntry(tarEntry: TarArchiveEntry): Graph[FlowShape[ByteString, ByteString], NotUsed] = {
    CacheFile.flow(filename = Paths.get(tarEntry.getName),
      expectedSize = tarEntry.getSize,
      saveFile = true,
      useSelfDeletingTempFile = true)
  }

  private def tarArchiveEntriesFromTarFile(tarSource: Source[ByteString, NotUsed]): Source[(TarArchiveInputStream, TarArchiveEntry), NotUsed] = {
    tarSource
      .toMat(StreamConverters.asInputStream())(Keep.right)
      .mapMaterializedValue(sourceFromTarInputStream)
      .run()
  }

  private def sourceFromTarInputStream(tarInputStream: InputStream): Source[(TarArchiveInputStream, TarArchiveEntry), NotUsed] = {
    val tarArchiveInputStream = new TarArchiveInputStream(tarInputStream)
    Source.unfold(tarArchiveInputStream) {
      tarArchiveInputStream: TarArchiveInputStream =>
        val nextTarEntry: TarArchiveEntry = tarArchiveInputStream.getNextTarEntry
        if (nextTarEntry != null)
          if (nextTarEntry.isCheckSumOK) {
            Some((tarArchiveInputStream, (tarArchiveInputStream, nextTarEntry)))
          } else {
            throw new TarFileCorruptedException
          }
        else
          None
    }
  }

  private def images_2017_07_tar_cache_flow = {
    CacheFile.flow(filename = Paths.get("images_2017_07.tar"),
      expectedSize = 3362037760L,
      expectedMd5 = Some(dehexify("bff4cdd922f018f343e53e2ffea909f2")),
      saveFile = true,
      useSelfDeletingTempFile = true)
  }

  private def images_2017_07_tar_gz_cache_flow = {
    CacheFile.flow(filename = Paths.get("images_2017_07.tar.gz"),
      expectedSize = 1038132176L,
      expectedMd5 = Some(dehexify("9c7d7d1b9c19f72c77ac0fa8e2695e00")),
      saveFile = true,
      useSelfDeletingTempFile = true)
  }

  private def images_2017_07_tar_gz_source = {
    Source.fromFuture(Http().singleRequest(HttpRequest(uri = "https://storage.googleapis.com/openimages/2017_07/images_2017_07.tar.gz")))
      .flatMapConcat(httpResponse => httpResponse.entity.withSizeLimit(1038132176).dataBytes)
  }

  def makeRequest(line: Map[String, ByteString]) = HttpRequest(uri = line("OriginalURL").utf8String)

  def makeDownloadUrlToFile(line: Map[String, ByteString], outputDir: Path, checkMd5IfExists: Boolean): DownloadParams = {
    val originalURLStr: String = line("OriginalURL").utf8String
    val originalURL: Uri = Uri(originalURLStr)
    val destPath: Path = outputDir.resolve(originalURL.path.toString().split('/').last)
    DownloadParams(originalURLStr, destPath, line("OriginalSize").utf8String.toLong,
      line("OriginalMD5").utf8String, checkMd5IfExists)
  }

  def outputDir(line: Map[String, ByteString]): java.nio.file.Path = Paths.get("2017_07", line("Subset").utf8String, "images-original")

  private def needToDownload(downloadUrlToFile: DownloadParams, alwaysDownload: Boolean): Future[Boolean] = {
    if (alwaysDownload) {
      Future(true)
    } else {
      Try(Files.size(downloadUrlToFile.filePath)) match {
        case Success(size) =>
          if (size != downloadUrlToFile.expectedSize) {
            Future(true)
          } else {
            // the file exists and the size matches, now we need to verify the md5
            if (downloadUrlToFile.checkMd5IfExists) {
              val fileSource: Source[ByteString, Future[IOResult]] = FileIO.fromPath(downloadUrlToFile.filePath)
              val md5CalculatorSink: Sink[ByteString, Future[DigestResult]] = DigestCalculator.sink(Algorithm.MD5)
              fileSource.runWith(md5CalculatorSink).map { md5DigestResult =>
                log.info("checked md5sum of {}. got {}, expected {}",
                  downloadUrlToFile.filePath, hexify(md5DigestResult.messageDigest), hexify(decodeBase64Md5(downloadUrlToFile.expectedMd5)))
                md5DigestResult.messageDigest != decodeBase64Md5(downloadUrlToFile.expectedMd5)
              }
            } else {
              Future(false)
            }
          }

        case Failure(_) => Future(true)
      }
    }
  }

}

case class InvalidSizeException(url: String, serverSize: Long, expectedSize: Long) extends
  RuntimeException("%s: server size (%d) doesn't match expected size (%d)!".format(url, serverSize, expectedSize))

case class NoContentLengthHeaderException(message: String) extends RuntimeException(message)

case class ContentTypeNotJpegException(message: String) extends RuntimeException(message)

case class RedirectResponseException(url: String) extends RuntimeException(url)

case class RequestFailedException(url: String, body: String) extends RuntimeException("http request to %s failed: %s".format(url, body))

case class MD5SumMismatch(downloadParams: DownloadParams) extends RuntimeException
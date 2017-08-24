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

case class Director(implicit system: ActorSystem) {
  val log = Logging(system, this.getClass)
  final implicit val materializer: ActorMaterializer = ActorMaterializer()

  def run(): Future[Done] = {
    val checkMd5IfExists = false
    val alwaysDownload = true

    val tarSource: Source[ByteString, NotUsed] = images_2017_07_tar_gz_source
      .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Stop))
      .via(images_2017_07_tar_gz_cache_flow)
      .via(Compression.gunzip())
      .via(images_2017_07_tar_cache_flow)

    val tarArchiveEntrySource: Source[(TarArchiveInputStream, TarArchiveEntry), NotUsed] = tarArchiveEntriesFromTarFile(tarSource)

    tarArchiveEntrySource
      .via(alsoToEagerCancelGraph(createDirsForEntrySink)) // don't use .alsoTo(), use Broadcast( , eagerCancel=true) to avoid consuming the entire input stream when downstream cancels.
      .flatMapConcat(tarArchiveEntryToCsvLines)
      .take(1000) // for testing purposes, limit to 10
      .alsoTo(countAndPrintNumberOfLinesPerType)
      .map(makeDownloadRequestTuple(checkMd5IfExists, _))
      .via(filterNeedToDownload(alwaysDownload))
      .via(Http().superPool())
      .mapAsyncUnordered(100)(processDownload)
      .watchTermination()(makeTerminationHandler)
      .runForeach(downloadComplete())
  }

  private def processDownload: ((Try[HttpResponse], DownloadUrlToFile)) => Future[Try[(IOResult, DigestResult, DownloadUrlToFile)]] = {

    // server replied with a 200 OK
    case (Success(resp@HttpResponse(StatusCodes.OK, headers, entity1, _)), downloadParams@DownloadUrlToFile(url: String, filePath: Path, expectedSize: Long, expectedMd5: String, checkMd5IfExists: Boolean)) =>
      log.info("{} OK. Content-Type: {}", url, entity1.contentType)
      val entity = entity1.withoutSizeLimit()
      entity.contentType match {
        case Binary(MediaTypes.`image/jpeg`) =>
          entity.contentLengthOption match {
            case Some(serverSize) =>
              if (serverSize == expectedSize) {
                val tupleOfFutures = entity.dataBytes
                  .alsoToMat(resizeImageAndSaveToFile(filePath))(Keep.right)
                  .toMat(saveToFileAndComputeMd5(filePath))(Keep.both)
                  .run()

                // need to wait for the resize and save file to finish, so create a joined future
                val joinedFuture = for {v1 <- tupleOfFutures._1; v2 <- tupleOfFutures._2} yield (v1, v2)

                // convert it to a Try and include downloadParams.
                joinedFuture
                  .map({
                    case (done, (ioResult, digestResult)) => Success((ioResult, digestResult, downloadParams))
                  })
                  .recover({
                    case t => Failure(t)
                  })
              } else {
                log.error("{}: server size ({}) doesn't match expected size ({})!", url, serverSize, expectedSize)
                resp.entity.discardBytes()
                Future.successful(Failure(InvalidSizeException(url, serverSize, expectedSize)))
              }
            case None =>
              log.error("{}: No Content-Length!", url)
              resp.entity.discardBytes()
              Future.successful(Failure(NoContentLengthHeaderException(url)))
          }
        case _ =>
          log.error("{}: Content-Type != image/jpeg", url)
          resp.entity.discardBytes()
          Future.successful(Failure(ContentTypeNotJpegException(url)))
      }


    // handle redirects
    case (Success(resp@HttpResponse(Redirection(_), _, _, _)), downloadParam: DownloadUrlToFile) =>
      resp.header[Location] match {
        case Some(location) =>
          log.info("{}: location: {}", downloadParam.url, location)
          if (location.uri.path.toString().endsWith("/photo_unavailable.png") || location.uri.path.toString.endsWith("/photo_unavailable_l.png")) {
            log.info("Got a photo unavailable redirect.")
            resp.entity.discardBytes()
          } else {
            resp.entity.discardBytes()
          }
        case None =>
          log.error("{}: Got redirect without Location header", downloadParam.url)
          resp.entity.discardBytes()
      }
      Future.successful(Failure(RedirectResponseException(downloadParam.url)))

    // handle failures
    case (Success(resp@HttpResponse(code, _, _, _)), downloadParam: DownloadUrlToFile) =>
      log.error("{}: Request failed, response code: {}", downloadParam.url, code)
      resp.entity.dataBytes.take(1024 * 1024).runFold(ByteString(""))(_ ++ _).map { body =>
        log.error("{}: failure body: {}", downloadParam.url, body.utf8String)
        Failure(RequestFailedException(downloadParam.url, body.utf8String))
      }

    case (Failure(exception), downloadParam: DownloadUrlToFile) =>
      log.error("{}: Request failed: {}", downloadParam.url, exception)
      Future.successful(Failure(exception))

  }

  private def downloadComplete(): (Try[(IOResult, DigestResult, DownloadUrlToFile)]) => Unit = {
    case Success((IOResult(count, Success(_)), md5DigestResult: DigestResult, downloadParams: DownloadUrlToFile)) =>
      val decodedMd5: ByteString = decodeBase64Md5(downloadParams.expectedMd5)

      if (decodedMd5 != md5DigestResult.messageDigest) {
        log.error("{} md5 sum doesn't match expected. actual: {}, expected: {}", downloadParams.url, hexify(md5DigestResult.messageDigest),
          hexify(decodedMd5))
        val deleted = Files.deleteIfExists(downloadParams.filePath)
        if (deleted) {
          log.info("Deleted {}", downloadParams.filePath)
        }
      }
      log.info("Request data completely read for {}. {} bytes.", downloadParams.url, count)
    case Success((IOResult(_, Failure(error)), _, downloadParams: DownloadUrlToFile)) =>
      log.error("{}: failed saving request data to file: {}", downloadParams.url, error)
      val deleted = Files.deleteIfExists(downloadParams.filePath)
      if (deleted) {
        log.info("Deleted {}", downloadParams.filePath)
      }
    case Failure(exception) =>
      log.error("Download failure: {}", exception)
  }

  private def resizeImageAndSaveToFile(filePath: Path): Sink[ByteString, Future[Done]] = {
    ImageResize.resizeJpgFlow(299).watchTermination()({
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
      .to(FileIO.toPath(resizedImagePath(filePath)))
  }

  private def saveToFileAndComputeMd5(filePath: Path): Sink[ByteString, Future[(IOResult, DigestResult)]] = {
    broadcastToSinksSingleFuture(FileIO.toPath(filePath), md5Sink)
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
          log.error("Failed: {}", error)
          System.err.println("Failed: " + error)
        case Success(_) =>
          log.info("done processing csv")
      })

  }

  private def filterNeedToDownload(alwaysDownload: Boolean): Flow[(HttpRequest, DownloadUrlToFile), (HttpRequest, DownloadUrlToFile), NotUsed] =
    Flow[(HttpRequest, DownloadUrlToFile)]
      .mapAsyncUnordered(Runtime.getRuntime.availableProcessors() * 2)(needToDownloadRequest(alwaysDownload))
      .filter(_._1)
      .map(_._2)

  private def needToDownloadRequest(alwaysDownload: Boolean): ((HttpRequest, DownloadUrlToFile)) => Future[(Boolean, (HttpRequest, DownloadUrlToFile))] = {
    case downloadRequestTuple@(downloadRequest: HttpRequest, downloadUrlToFile: DownloadUrlToFile) =>
      for {
        nd <- needToDownload(downloadUrlToFile, alwaysDownload)
      } yield (nd, downloadRequestTuple)
  }

  private def makeDownloadRequestTuple(checkMd5IfExists: Boolean, line: Map[String, ByteString]): (HttpRequest, DownloadUrlToFile) = {
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

  def makeDownloadUrlToFile(line: Map[String, ByteString], outputDir: Path, checkMd5IfExists: Boolean): DownloadUrlToFile = {
    val originalURLStr: String = line("OriginalURL").utf8String
    val originalURL: Uri = Uri(originalURLStr)
    val destPath: Path = outputDir.resolve(originalURL.path.toString().split('/').last)
    DownloadUrlToFile(originalURLStr, destPath, line("OriginalSize").utf8String.toLong,
      line("OriginalMD5").utf8String, checkMd5IfExists)
  }

  def outputDir(line: Map[String, ByteString]): java.nio.file.Path = Paths.get("2017_07", line("Subset").utf8String, "images-original")

  private def needToDownload(downloadUrlToFile: DownloadUrlToFile, alwaysDownload: Boolean): Future[Boolean] = {
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
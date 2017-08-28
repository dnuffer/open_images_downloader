package nuffer.oidl

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

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

import scala.concurrent.duration._
import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.ExecutionContext
//import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

case class DownloadParams(url: String, filePath: Path, expectedSize: Long, expectedMd5: String, checkMd5IfExists: Boolean, csvLine: Map[String, ByteString])

case class ImageProcessingState(downloadParams: DownloadParams, needToDownload: Boolean)

trait MetadataFileDetails {
  def name: String

  def size: Long

  def md5sum: String
}

case class MetadataTarGzFile(name: String, size: Long, md5sum: String) extends MetadataFileDetails

case class MetadataTarFile(name: String, size: Long, md5sum: String) extends MetadataFileDetails

case class MetadataFile(description: String, uri: String, tarGzFile: MetadataTarGzFile, tarFile: MetadataTarFile)

case class DatasetMetadata(imagesFile: MetadataFile, otherFiles: Iterable[MetadataFile])

object DatasetMetadata {
  val openImagesV2DatasetMetadata = DatasetMetadata(
    imagesFile = MetadataFile(
      description = "Image URLs and metadata",
      uri = "https://storage.googleapis.com/openimages/2017_07/images_2017_07.tar.gz",
      tarGzFile = MetadataTarGzFile(
        name = "images_2017_07.tar.gz",
        size = 1038132176,
        md5sum = "9c7d7d1b9c19f72c77ac0fa8e2695e00"
      ),
      tarFile = MetadataTarFile(
        name = "images_2017_07.tar",
        size = 3362037760L,
        md5sum = "bff4cdd922f018f343e53e2ffea909f2"
      )
    ),
    otherFiles = List(
      MetadataFile(
        description = "Bounding box annotations (train, validation, and test sets)",
        uri = "https://storage.googleapis.com/openimages/2017_07/annotations_human_bbox_2017_07.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "annotations_human_bbox_2017_07.tar.gz",
          size = 38732419,
          md5sum = "3c57bff20c56e50025e16562c4c0d709"
        ),
        tarFile = MetadataTarFile(
          name = "annotations_human_bbox_2017_07.tar",
          size = 141342720,
          md5sum = "927db28263ee14c741d2e7c08cdd15c7"
        )
      ),
      MetadataFile(
        description = "Human-verified image-level annotations (train, validation, and test sets)",
        uri = "https://storage.googleapis.com/openimages/2017_07/annotations_human_2017_07.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "annotations_human_2017_07.tar.gz",
          size = 69263638,
          md5sum = "27610fb9d349b728448507beec244e9f"
        ),
        tarFile = MetadataTarFile(
          name = "annotations_human_2017_07.tar",
          size = 415528960,
          md5sum = "31f0e58103335281d8413a67bfd526b9"
        )
      ),
      MetadataFile(
        description = "Machine-generated image-level annotations (train, validation, and test sets)",
        uri = "https://storage.googleapis.com/openimages/2017_07/annotations_machine_2017_07.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "annotations_machine_2017_07.tar.gz",
          size = 468937924,
          md5sum = "c19b3fc5059ac13431b085c3e5df08af"
        ),
        tarFile = MetadataTarFile(
          name = "annotations_machine_2017_07.tar",
          size = 3127920640L,
          md5sum = "7825a907b569ccd3df88002936bead68"
        )
      ),
      MetadataFile(
        description = "Classes and class descriptions",
        uri = "https://storage.googleapis.com/openimages/2017_07/classes_2017_07.tar.gz",
        tarGzFile = MetadataTarGzFile(
          name = "classes_2017_07.tar.gz",
          size = 300021,
          md5sum = "a51205f02faf3867ae2d3e8dc968fd6d"
        ),
        tarFile = MetadataTarFile(
          name = "classes_2017_07.tar",
          size = 727040,
          md5sum = "20f621858305d3d56a1607a64ace4dcc"
        )
      ),
    )
  )

}

case class Director(implicit system: ActorSystem) {
  val log = Logging(system, this.getClass)
  final implicit val materializer: ActorMaterializer = ActorMaterializer()

  def run(): Future[Done] = {
    val otherFilesFuture: Future[Done] = Future.sequence(
      DatasetMetadata.openImagesV2DatasetMetadata.otherFiles.take(1)
        .map(downloadAndExtractMetadataFile)
    ).map(_ => Done)

    val imagesFuture: Future[Done] = startDownloadingImages

    for {
      otherFiles <- otherFilesFuture
      images <- imagesFuture
    } yield Done
  }

  private def startDownloadingImages = {
    val checkMd5IfExists = false
    val alwaysDownload = false

    val tarSource: Source[ByteString, NotUsed] = metadataFileSource(DatasetMetadata.openImagesV2DatasetMetadata.imagesFile)
      .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Stop))
      .via(metadataFileCacheFlow(DatasetMetadata.openImagesV2DatasetMetadata.imagesFile.tarGzFile))
      .via(Compression.gunzip())
      .via(metadataFileCacheFlow(DatasetMetadata.openImagesV2DatasetMetadata.imagesFile.tarFile))

    val tarArchiveEntrySource: Source[(TarArchiveInputStream, TarArchiveEntry), NotUsed] = tarArchiveEntriesFromTarFile(tarSource)

    val imagesFuture: Future[Done] = tarArchiveEntrySource
      .via(alsoToEagerCancelGraph(createDirsForEntrySink)) // don't use .alsoTo(), use Broadcast( , eagerCancel=true) to avoid consuming the entire input stream when downstream cancels.
      .alsoTo(Sink.foreach(createResultsCsv))
      .flatMapConcat(tarArchiveEntryToCsvLines)
      .take(100) // for testing purposes, limit to 100
      .alsoTo(countAndPrintNumberOfLinesPerType)
      .map(makeDownloadRequestTuple(checkMd5IfExists, _))
      .via(mapNeedToDownloadRequest(alwaysDownload))
      .via(startDownloadOrFromFile)
      .mapAsyncUnordered(100 /* TODO: make this the same size as the number of http streams */)(processJpegBytes)
      .via(Flow.fromFunction(processMd5Result))
      .alsoTo(Sink.foreach(writeResultToCsv))
      .watchTermination()(makeTerminationHandler)
      .runWith(Sink.ignore)
    //      .runWith(Sink.foreach(println))
    imagesFuture
  }

  private def downloadAndExtractMetadataFile: (MetadataFile) => Future[Done] = {
    metadataFile => {
      val annotationsSource: Source[ByteString, NotUsed] = metadataFileSource(metadataFile)
        .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.stop))
        .via(metadataFileCacheFlow(metadataFile.tarGzFile))
        .via(Compression.gunzip())
        .via(metadataFileCacheFlow(metadataFile.tarFile))

      val annotationsTarArchiveEntrySource: Source[(TarArchiveInputStream, TarArchiveEntry), NotUsed] = tarArchiveEntriesFromTarFile(annotationsSource)

      annotationsTarArchiveEntrySource
        .via(alsoToEagerCancelGraph(createDirsForEntrySink))
        .runForeach({
          case ((tarArchiveInputStream: TarArchiveInputStream, tarArchiveEntry: TarArchiveEntry)) =>
            Await.result(StreamConverters.fromInputStream(() => TarEntryInputStream(tarArchiveInputStream))
              .runWith(FileIO.toPath(Paths.get(tarArchiveEntry.getName))),
              Duration.Inf)

        })
      //        // This is async so the waiting can be handled by the framework.
      //        // Need to limit to parallelism 1 because the TarArchiveInputStream can only by read
      //        // once and has to be processed in sequence.
      //        .mapAsync(1)(
      //        {
      //          case ((tarArchiveInputStream, tarArchiveEntry)) =>
      //            StreamConverters.fromInputStream(() => TarEntryInputStream(tarArchiveInputStream))
      //              .runWith(FileIO.toPath(Paths.get(tarArchiveEntry.getName)))
      ////              .via(cacheFileForTarEntry(tarArchiveEntry))
      ////              .toMat(Sink.ignore)(Keep.left)
      ////              .run()
      //        })
      //        .runWith(Sink.ignore)
    }
  }

  val csvFilenameKey: String = "_csv_filename"

  val imagesSuccessCsvFilename = "images-success.csv"
  val imagesFailureCsvFilename = "images-failure.csv"

  private def quoteAuthorAndTitle(line: String, columnName: String): String = {
    if (columnName == "Author" || columnName == "Title")
      quote(line)
    else
      line
  }

  private def quote(line: String) = {
    '"' + line.replace("\"", "\"\"") + '"'
  }

  private def writeResultToCsv: Try[ImageProcessingState] => Unit = {
    case Success(state) =>
      Files.write(Paths.get(state.downloadParams.csvLine(csvFilenameKey).utf8String).resolveSibling(imagesSuccessCsvFilename),
        csvHeaderArray.map(columnName => quoteAuthorAndTitle(state.downloadParams.csvLine(columnName).utf8String, columnName)).mkString("", ",", "\n").getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND, StandardOpenOption.WRITE)
    case Failure(exception: ImageProcessingException) =>
      val csvLine = exception.state.downloadParams.csvLine.updated("FailureDetails", ByteString(quote(exception.getMessage)))
      Files.write(Paths.get(csvLine(csvFilenameKey).utf8String).resolveSibling(imagesFailureCsvFilename),
        failureCsvHeaderArray.map(columnName => quoteAuthorAndTitle(csvLine(columnName).utf8String, columnName)).mkString("", ",", "\n").getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND, StandardOpenOption.WRITE)
    case Failure(exception: Throwable) =>
      log.error("writeResultToCsv got an unhandled exception: {}", exception)
  }

  val csvHeader: String = "ImageID,Subset,OriginalURL,OriginalLandingURL,License,AuthorProfileURL,Author,Title,OriginalSize,OriginalMD5,Thumbnail300KURL\n"
  val csvHeaderArray: Array[String] = csvHeader.substring(0, csvHeader.length - 1).split(',')

  val failureCsvHeader: String = csvHeader.substring(0, csvHeader.length - 1) + ",FailureDetails\n"
  val failureCsvHeaderArray: Array[String] = failureCsvHeader.substring(0, failureCsvHeader.length - 1).split(',')

  private def createResultsCsv: ((TarArchiveInputStream, TarArchiveEntry)) => Unit = {

    case (tarArchiveInputStream: TarArchiveInputStream, tarArchiveEntry: TarArchiveEntry) =>
      val entryPath = Paths.get(tarArchiveEntry.getName)
      val successPath = entryPath.resolveSibling(imagesSuccessCsvFilename)
      Files.write(successPath, csvHeader.getBytes(StandardCharsets.UTF_8))
      val failurePath = entryPath.resolveSibling(imagesFailureCsvFilename)
      Files.write(failurePath, failureCsvHeader.getBytes(StandardCharsets.UTF_8))
  }

  private def startDownloadOrFromFile: Graph[FlowShape[(HttpRequest, ImageProcessingState), Try[(Source[ByteString, Any], ImageProcessingState)]], NotUsed] = {
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

      // convert it to a Try and include state.
      joinedFuture
        .map({
          case (done, (ioResult, digestResult)) => Success((ioResult, digestResult, state))
        })
        .recover({
          case exception => Failure(ImageUnknownException(state, exception))
        })
    case Failure(exception: ImageProcessingException) =>
      Future.successful(Failure(exception))
    case Failure(exception) =>
      log.error("processJpegBytes unknown exception: {}", exception)
      Future.successful(Failure(exception))
  }

  private def keepSuccess[T]: (Try[T]) => Boolean = {
    case Success(_) => true
    case _ => false
  }

  private def processHttpResponse: ((Try[HttpResponse], ImageProcessingState)) => Future[Try[(Source[ByteString, Any], ImageProcessingState)]] = {

    // server replied with a 200 OK
    case (Success(HttpResponse(StatusCodes.OK, _, entity1, _)), state@ImageProcessingState(downloadParams@DownloadParams(url: String, _, expectedSize: Long, _, _, _), _)) =>
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
                handleResponseError(entity, InvalidSizeException(url, serverSize, expectedSize, state))
              }
            case None =>
              log.error("{}: No Content-Length!", url)
              handleResponseError(entity, NoContentLengthHeaderException(url, state))
          }
        case _ =>
          log.error("{}: Content-Type != image/jpeg", url)
          handleResponseError(entity, ContentTypeNotJpegException(url, state))
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
      handleResponseError(resp.entity, RedirectResponseException(String.format("photo is unavailable: %s", downloadParams.url), state))

    // handle failures
    case (Success(resp@HttpResponse(code, _, _, _)), state@ImageProcessingState(downloadParams, _)) =>
      log.error("{}: Request failed, response code: {}", downloadParams.url, code)
      resp.entity.withSizeLimit(1024 * 1024).dataBytes.take(1024 * 1024).runFold(ByteString(""))(_ ++ _).map { body =>
        log.error("{}: failure body: {}", downloadParams.url, body.utf8String)
        Failure(RequestFailedException(downloadParams.url, body.utf8String, state))
      } recover {
        case _ => Failure(RequestFailedException(downloadParams.url, "", state))
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
        Failure(MD5SumMismatch(state))
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
      Failure(ImageIOException(state, error))
    case Failure(exception: ImageProcessingException) =>
      Failure(exception)
    case Failure(exception) =>
      log.error("Download failure unknown exception: {}", exception)
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
      Files.createDirectories(filePath.getParent) // theoretically this should be done during materialization, but somehow that's a race condition with FileIO.toPath() and so causes random failures.
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
      Files.createDirectories(filePath.getParent) // theoretically this should be done during materialization, but somehow that's a race condition with FileIO.toPath() and so causes random failures.
      broadcastToSinksSingleFuture(FileIO.toPath(filePath), md5Sink)
    } else {
      md5Sink.mapMaterializedValue(_.map((IOResult(Files.size(filePath), Success(Done)), _)))
    }
  }

  private def resizedImagePath(filePath: Path): Path = {
    filePath.getParent.getParent.resolveSibling("images-resized").resolve(filePath.getParent.getFileName).resolve(filePath.getFileName)
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
        .map(line => line.updated(csvFilenameKey, ByteString(tarEntry.getName)))
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

  private def metadataFileSource(metadataFile: MetadataFile): Source[ByteString, NotUsed] = {
    httpGetDataSource(metadataFile.uri, metadataFile.tarGzFile.size)
  }

  private def metadataFileCacheFlow(metadataFileDetails: MetadataFileDetails): Graph[FlowShape[ByteString, ByteString], NotUsed] = {
    CacheFile.flow(filename = Paths.get(metadataFileDetails.name),
      expectedSize = metadataFileDetails.size,
      expectedMd5 = Some(dehexify(metadataFileDetails.md5sum)),
      saveFile = true,
      useSelfDeletingTempFile = true)
  }

  private def httpGetDataSource(uri: String, fileSize: Long): Source[ByteString, NotUsed] = {
    Source.fromFuture(Http().singleRequest(HttpRequest(uri = uri)))
      .flatMapConcat(httpResponse => httpResponse.entity.withSizeLimit(fileSize).dataBytes)
  }

  def makeRequest(line: Map[String, ByteString]) = HttpRequest(uri = line("OriginalURL").utf8String)

  def makeDownloadUrlToFile(line: Map[String, ByteString], outputDir: Path, checkMd5IfExists: Boolean): DownloadParams = {
    val originalURLStr: String = line("OriginalURL").utf8String
    val originalURL: Uri = Uri(originalURLStr)
    val fileName = originalURL.path.toString().split('/').last
    val fileDirName = fileName.substring(0, 3) // the first 3 chars will create 1000 dirs with 9000 files in each. That's pretty balanced.
    val destPath: Path = outputDir.resolve(fileDirName).resolve(fileName)
    DownloadParams(originalURLStr, destPath, line("OriginalSize").utf8String.toLong,
      line("OriginalMD5").utf8String, checkMd5IfExists, line)
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

sealed trait ImageProcessingException {
  def state: ImageProcessingState
}

case class InvalidSizeException(url: String, serverSize: Long, expectedSize: Long, state: ImageProcessingState) extends
  RuntimeException("%s: server size (%d) doesn't match expected size (%d)!".format(url, serverSize, expectedSize)) with
  ImageProcessingException

case class NoContentLengthHeaderException(message: String, state: ImageProcessingState) extends RuntimeException(message) with ImageProcessingException

case class ContentTypeNotJpegException(message: String, state: ImageProcessingState) extends RuntimeException(message) with ImageProcessingException

case class RedirectResponseException(message: String, state: ImageProcessingState) extends RuntimeException(message) with ImageProcessingException

case class RequestFailedException(url: String, body: String, state: ImageProcessingState) extends RuntimeException("http request to %s failed: %s".format(url, body)) with ImageProcessingException

case class MD5SumMismatch(state: ImageProcessingState) extends RuntimeException("md5 sum mismatch") with ImageProcessingException

case class ImageIOException(state: ImageProcessingState, cause: Throwable) extends RuntimeException(cause) with ImageProcessingException

case class ImageUnknownException(state: ImageProcessingState, cause: Throwable) extends RuntimeException(cause) with ImageProcessingException
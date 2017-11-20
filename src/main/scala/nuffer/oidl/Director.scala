package nuffer.oidl

import java.nio.channels.SeekableByteChannel
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
import akka.stream.scaladsl.{Broadcast, Compression, Concat, FileIO, Flow, GraphDSL, Keep, Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import nuffer.oidl.ResizeMode.ResizeMode
import nuffer.oidl.Utils.{broadcastToSinksSingleFuture, decodeBase64Md5, dehexify, hexify}
import org.apache.commons.compress.archivers.tar.TarArchiveEntry

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case class DownloadParams(url: String, filePath: Path, expectedSize: Long, expectedMd5: String, checkMd5IfExists: Boolean, csvLine: Map[String, ByteString])

case class ImageProcessingState(downloadParams: DownloadParams, needToDownload: Boolean)

case class Director(rootDir: Path,
                    originalImagesSubdirectory: String,
                    checkMd5IfExists: Boolean,
                    alwaysDownload: Boolean,
                    saveTarBalls: Boolean,
                    downloadMetadata: Boolean,
                    downloadImages: Boolean,
                    download300K: Boolean,
                    saveOriginalImages: Boolean,
                    resizeImages: Boolean,
                    resizedImagesSubdirectory: String,
                    resizeMode: ResizeMode,
                    resizeBoxSize: Long,
                    resizeOutputFormat: String,
                    resizeCompressionQuality: Option[Long],
                    dataVersion: Long)(implicit system: ActorSystem) {
  val log = Logging(system, this.getClass)
  final implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(java.util.concurrent.Executors.newCachedThreadPool())
  final implicit val materializer: ActorMaterializer = ActorMaterializer()
  val datasetMetadata: DatasetMetadata = dataVersion match {
    case 1 => DatasetMetadata.openImagesV1DatasetMetadata
    case 2 => DatasetMetadata.openImagesV2DatasetMetadata
    case 3 => DatasetMetadata.openImagesV3DatasetMetadata
  }

  def run(): Future[Done] = {

    val otherFilesFuture: Future[Done] = if (downloadMetadata) {
      Future.sequence(
        datasetMetadata.otherFiles
          .map(downloadAndExtractMetadataFile)
      ).map(_ => Done)
    } else {
      Future(Done)
    }

    val imagesFuture: Future[Done] =
      if (downloadImages) startDownloadingImages
      else Future(Done)

    for {
      _ <- otherFilesFuture
      _ <- imagesFuture
    } yield Done
  }

  private def startDownloadingImages: Future[Done] = {
    val csvLineFlow: Flow[ByteString, Map[String, ByteString], NotUsed] =
      TarArchive.subflowPerEntry(SubstreamCancelStrategy.drain)
        .via(alsoToEagerCancelGraph(
          Flow[(TarArchiveEntry, ByteString)]
            .map(tae => rootDir.resolve(Paths.get(tae._1.getName).getParent))
            .via(Unique.flow())
            .via(alsoToEagerCancelGraph(Sink.foreach(createResultsCsvInDir)))
            .to(Sink.foreach(dir => Files.createDirectories(dir)))
        ))
        .prefixAndTail(1)
        .map {
          case (seq: immutable.Seq[(TarArchiveEntry, ByteString)], source: Source[(TarArchiveEntry, ByteString), NotUsed]) =>
            (seq.head, Source.combine(Source(seq), source)(Concat(_)))
        }
        .filter({ case ((tae: TarArchiveEntry, _: ByteString), _: Source[(TarArchiveEntry, ByteString), NotUsed]) => tae.isFile })
        .map {
          case ((tae: TarArchiveEntry, _: ByteString), source: Source[(TarArchiveEntry, ByteString), NotUsed]) =>
            source
              .map(_._2)
              .via(cacheFileForTarEntry(tae))
        }
        .flatMapConcat(source => source
          .via(csvLineScanner)
          .via(CsvToMap.toMap())
          // TODO: maybe add this back in using unzip/zip in a custom graph to pass the tarEntry down to the point.
          //            .map(_.updated(csvFilenameKey, ByteString(tarEntry.getName)))
        ).concatSubstreams

    metadataFileSource(datasetMetadata.imagesFile)
      .log("images http source").withAttributes(Attributes.logLevels(onFinish = Logging.InfoLevel, onElement = Logging.InfoLevel))
      .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Stop))
      .via(metadataFileCacheFlow(datasetMetadata.imagesFile.tarGzFile))
      .log("images.tar.gz cache").withAttributes(Attributes.logLevels(onFinish = Logging.InfoLevel))
      .via(Compression.gunzip())
      .via(metadataFileCacheFlow(datasetMetadata.imagesFile.tarFile))
      .log("images.tar cache").withAttributes(Attributes.logLevels(onFinish = Logging.InfoLevel))
      .via(csvLineFlow)
//            .take(100) // for testing purposes, limit to 100
      .alsoTo(countAndPrintNumberOfLinesPerType)
      .map(makeDownloadRequestTuple(checkMd5IfExists, _))
      .via(mapNeedToDownloadRequest(alwaysDownload))
      .via(startDownloadOrFromFile)
      .mapAsyncUnordered(100 /* TODO: make this the same size as the number of http streams */)(processJpegBytes)
      .via(Flow.fromFunction(processMd5Result))
      .alsoTo(Sink.foreach(writeResultToCsv(datasetMetadata.topDir)))
      .watchTermination()(makeTerminationHandler)
      .runWith(Sink.ignore)
  }

  private def csvLineScanner: Graph[FlowShape[ByteString, List[ByteString]], NotUsed] = {
    CsvParsing.lineScanner(CsvParsing.Comma, CsvParsing.DoubleQuote, '\0')
  }

  private def getExtractTarArchiveEntryDataToFiles: () => ((TarArchiveEntry, ByteString, TarArchiveEntryRecordOrder)) => List[IOResult] = () => {
    var chan: Option[SeekableByteChannel] = None
    var bytesWritten: Long = 0
    (_: (TarArchiveEntry, ByteString, TarArchiveEntryRecordOrder)) match {
      case (tarArchiveEntry: TarArchiveEntry, bytes: ByteString, order: TarArchiveEntryRecordOrder) =>
        if (order.first) {
          log.info(s"Got first record for ${tarArchiveEntry.getName}. Size: ${tarArchiveEntry.getSize}")
          val path = rootDir.resolve(Paths.get(tarArchiveEntry.getName))
          if (tarArchiveEntry.isDirectory) {
            Files.createDirectories(path)
          }
          chan = if (tarArchiveEntry.isFile && !fileHasSize(path, tarArchiveEntry.getSize)) {
            Files.createDirectories(path.getParent)
            bytesWritten = 0
            Some(Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))
          } else {
            None
          }
        }

        try {
          chan.foreach(_.write(bytes.toByteBuffer))
          bytesWritten += bytes.length
          if (order.last) {
            chan.foreach(_.close())
            chan = None
            val copyOfBytesWritten = bytesWritten
            bytesWritten = 0
            if (copyOfBytesWritten != tarArchiveEntry.getSize) {
              val message = s"expected ${tarArchiveEntry.getSize} bytes, got $copyOfBytesWritten"
              List(IOResult(copyOfBytesWritten, Failure(TarFileEntryIncorrectSizeException(message))))
            } else {
              List(IOResult(copyOfBytesWritten, Success(Done)))
            }
          } else {
            List()
          }
        }
        catch {
          case NonFatal(exception) => {
            chan.foreach(_.close())
            chan = None
            val copyOfBytesWritten = bytesWritten
            bytesWritten = 0
            List(IOResult(copyOfBytesWritten, Failure(exception)))
          }
        }
    }
  }

  private def downloadAndExtractMetadataFile: (MetadataFile) => Future[Done] = {
    metadataFile => {
      metadataFileSource(metadataFile)
        .log(metadataFile.uri + " http source").withAttributes(Attributes.logLevels(onFinish = Logging.InfoLevel, onElement = Logging.InfoLevel))
        .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.stop))
        .via(metadataFileCacheFlow(metadataFile.tarGzFile))
        .log(metadataFile.tarGzFile.name + " cache").withAttributes(Attributes.logLevels(onFinish = Logging.InfoLevel))
        .via(Compression.gunzip())
        .via(metadataFileCacheFlow(metadataFile.tarFile))
        .log(metadataFile.tarFile.name + " cache").withAttributes(Attributes.logLevels(onFinish = Logging.InfoLevel))
        .via(TarArchive.flow())
        .log(metadataFile.tarFile.name + " entries and data").withAttributes(Attributes.logLevels(onFinish = Logging.InfoLevel))
        .statefulMapConcat(getExtractTarArchiveEntryDataToFiles)
        .log(metadataFile.tarFile.name + " IOResults").withAttributes(Attributes.logLevels(onFinish = Logging.InfoLevel, onElement = Logging.InfoLevel))
        .runWith(Sink.fold(Done)({
          case (Done, IOResult(_, Success(Done))) =>
            Done
          // If a write failed and an IOResult failure happened, fail the stream
          case (Done, IOResult(_, Failure(exception))) =>
            throw exception
        }))
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

  private def writeResultToCsv(datasetTopDir: String): Try[ImageProcessingState] => Unit = {
    case Success(state) =>
      Files.write(rootDir.resolve(datasetTopDir).resolve(state.downloadParams.csvLine("Subset").utf8String).resolve(imagesSuccessCsvFilename),
        csvHeaderArray.map(columnName => quoteAuthorAndTitle(state.downloadParams.csvLine(columnName).utf8String, columnName)).mkString("", ",", "\n").getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND, StandardOpenOption.WRITE)
    case Failure(exception: ImageProcessingException) =>
      val csvLine = exception.state.downloadParams.csvLine.updated("FailureDetails", ByteString(quote(exception.getMessage)))
      Files.write(rootDir.resolve(datasetTopDir).resolve(csvLine("Subset").utf8String).resolve(imagesFailureCsvFilename),
        failureCsvHeaderArray.map(columnName => quoteAuthorAndTitle(csvLine(columnName).utf8String, columnName)).mkString("", ",", "\n").getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND, StandardOpenOption.WRITE)
    case Failure(exception: Throwable) =>
      log.error("writeResultToCsv got an unhandled exception: {}", exception)
  }

  val csvHeader: String = "ImageID,Subset,OriginalURL,OriginalLandingURL,License,AuthorProfileURL,Author,Title,OriginalSize,OriginalMD5,Thumbnail300KURL\n"
  val csvHeaderArray: Array[String] = csvHeader.substring(0, csvHeader.length - 1).split(',')

  val failureCsvHeader: String = csvHeader.substring(0, csvHeader.length - 1) + ",FailureDetails\n"
  val failureCsvHeaderArray: Array[String] = failureCsvHeader.substring(0, failureCsvHeader.length - 1).split(',')

  private def createResultsCsvInDir(dirPath: Path) = {
    val successPath = dirPath.resolve(imagesSuccessCsvFilename)
    Files.write(successPath, csvHeader.getBytes(StandardCharsets.UTF_8))
    val failurePath = dirPath.resolve(imagesFailureCsvFilename)
    Files.write(failurePath, failureCsvHeader.getBytes(StandardCharsets.UTF_8))
  }

  private def startDownloadOrFromFile: Graph[FlowShape[(HttpRequest, ImageProcessingState), Try[(Source[ByteString, Any], ImageProcessingState)]], NotUsed] = {
    IfThenElse.flow(
      {
        case (_: HttpRequest, state: ImageProcessingState) => state.needToDownload
      },
      Flow[(HttpRequest, ImageProcessingState)]
        .via(Http().superPool())
        .mapAsyncUnordered(100 /* TODO: make this the same size as the number of http streams */)(processHttpResponse)
      ,
      Flow[(HttpRequest, ImageProcessingState)].map({
        case (_: HttpRequest, state: ImageProcessingState) =>
          Success((FileIO.fromPath(state.downloadParams.filePath), state))
      })
    )
  }

  private def processJpegBytes: (Try[(Source[ByteString, Any], ImageProcessingState)]) => Future[Try[(IOResult, DigestResult, ImageProcessingState)]] = {
    case Success((dataBytes, state@ImageProcessingState(downloadParams, _))) =>
      val sourceWithOptionalResize: Source[ByteString, Future[Done]] =
        if (resizeImages) {
          dataBytes
            .alsoToMat(resizeImageAndSaveToFile(resizedImagePath(downloadParams.filePath)))(Keep.right)
        } else {
          dataBytes.mapMaterializedValue(_ => Future(Done))
        }

      val tupleOfFutures: (Future[Done], Future[(IOResult, DigestResult)]) = sourceWithOptionalResize
        .toMat(saveToFileIfNotPresentAndComputeMd5(downloadParams.filePath, state.needToDownload))(Keep.both)
        .run()

      // need to wait for the resize and save file to finish, so create a joined future
      val joinedFuture = for {v1 <- tupleOfFutures._1; v2 <- tupleOfFutures._2} yield (v1, v2)

      // convert it to a Try and include state.
      joinedFuture
        .map({
          case (_, (ioResult, digestResult)) => Success((ioResult, digestResult, state))
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

  private def processHttpResponse: ((Try[HttpResponse], ImageProcessingState)) => Future[Try[(Source[ByteString, Any], ImageProcessingState)]] = {

    // server replied with a 200 OK
    case (Success(HttpResponse(StatusCodes.OK, _, entity1, _)), state@ImageProcessingState(DownloadParams(url: String, _, expectedSize: Long, _, _, _), _)) =>
      log.info("{} OK. Content-Type: {}", url, entity1.contentType)
      val sizeLimit: Long = if (download300K) Math.max(1200000L, expectedSize) else expectedSize
      val entity = entity1.withSizeLimit(sizeLimit)
      entity.contentType match {
        case Binary(MediaTypes.`image/jpeg`) =>
          entity.contentLengthOption match {
            case Some(serverSize) =>
              // If we download 300K images, the size is unpredictable because the image is generated on the fly, so don't bother checking it.
              if (download300K || serverSize == expectedSize) {
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
          //          log.info("{}: location: {}", downloadParams.url, location)
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

    case (Failure(exception), ImageProcessingState(downloadParams, _)) =>
      log.error("{}: Request failed: {}", downloadParams.url, exception)
      Future.successful(Failure(exception))
  }

  private def handleResponseError(entity: ResponseEntity, exception: Throwable) = {
    //    val MTU_BYTES: Long = 1500 // since we've got at least one packet already, we can discard some without blocking, and possibly re-use the connection.
    // Sure there's the headers (~730 bytes from flickr.com) not being accounted for and transfer encoding, so this is just a hopefully-effective heuristic.
    val REDIRECT_BYTES: Long = 3213 // the not available redirect includes this many bytes. With headers, this should limit to reading 3 packets.
    entity.withSizeLimit(REDIRECT_BYTES).discardBytes().future().transform(_ => Success(Failure(exception)))
  }

  private def processMd5Result: Try[(IOResult, DigestResult, ImageProcessingState)] => Try[ImageProcessingState] = {
    case Success((IOResult(count, Success(_)), md5DigestResult: DigestResult, state@ImageProcessingState(downloadParams, _))) =>
      val decodedMd5: ByteString = decodeBase64Md5(downloadParams.expectedMd5)

      // If we download 300K images, the md5 is unpredictable because the image is generated on the fly, so don't bother checking it.
      if (!download300K && decodedMd5 != md5DigestResult.messageDigest) {
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
      case NonFatal(_) => false
    }

  def fileHasSize(filePath: Path, size: Long): Boolean =
    try {
      Files.size(filePath) == size
    } catch {
      case NonFatal(_) => false
    }

  private def resizeImageAndSaveToFile(filePath: Path): Sink[ByteString, Future[Done]] = {
    if (fileExistsAndHasGreaterThanZeroSize(filePath)) {
      Sink.ignore
    } else {
      Files.createDirectories(filePath.getParent) // theoretically this should be done during materialization, but somehow that's a race condition with FileIO.toPath() and so causes random failures.
      val resizeFlow = resizeMode match {
        case ResizeMode.ShrinkToFit => ImageResize.resizeShrinkToFitJpegFlow(resizeBoxSize, resizeOutputFormat, resizeCompressionQuality)
        case ResizeMode.FillCrop => ImageResize.resizeFillCropJpegFlow(resizeBoxSize, resizeOutputFormat, resizeCompressionQuality)
        case ResizeMode.FillDistort => ImageResize.resizeFillDistortJpegFlow(resizeBoxSize, resizeOutputFormat, resizeCompressionQuality)
      }
      resizeFlow
        .watchTermination()({
          case (mat, eventualDone: Future[Done]) =>
            eventualDone.onComplete({
              case Failure(error) =>
                log.error("Resize of {} failed: {}", filePath, error)
              case Success(_) =>
                log.info("Done resizing {}", filePath)
            })
            mat
        })
        .to(FileIO.toPath(filePath))
    }
  }

  private def saveToFileIfNotPresentAndComputeMd5(filePath: Path, needToDownload: Boolean): Sink[ByteString, Future[(IOResult, DigestResult)]] = {
    if (needToDownload && saveOriginalImages) {
      Files.createDirectories(filePath.getParent) // theoretically this should be done during materialization, but somehow that's a race condition with FileIO.toPath() and so causes random failures.
      broadcastToSinksSingleFuture(FileIO.toPath(filePath), md5Sink())
    } else {
      val size = try Files.size(filePath) catch {
        case NonFatal(_) => 0
      }
      md5Sink().mapMaterializedValue(_.map((IOResult(size, Success(Done)), _)))
    }
  }

  private def resizedImagePath(filePath: Path): Path = {
    val subsetDir = filePath.getParent.getParent
    val resizedDir = subsetDir.resolveSibling(resizedImagesSubdirectory)
    val threeCharPrefix = filePath.getParent.getFileName
    val resizedImageDir = resizedDir.resolve(threeCharPrefix)
    val imageFilename = filePath.getFileName
    val filenameStr = imageFilename.toString
    if (filenameStr.endsWith(".jpg"))
      resizedImageDir.resolve(filenameStr.substring(0, filenameStr.length - 4) + "." + resizeOutputFormat)
    else
      resizedImageDir.resolve(filenameStr + "." + resizeOutputFormat)
  }

  private def md5Sink(): Sink[ByteString, Future[DigestResult]] = {
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

  private def mapNeedToDownloadRequest(alwaysDownload: Boolean): Flow[(HttpRequest, DownloadParams), (HttpRequest, ImageProcessingState), NotUsed] = {
    Flow[(HttpRequest, DownloadParams)]
      .mapAsyncUnordered(Runtime.getRuntime.availableProcessors() * 2)(needToDownloadRequest(alwaysDownload))
  }

  private def needToDownloadRequest(alwaysDownload: Boolean): ((HttpRequest, DownloadParams)) => Future[(HttpRequest, ImageProcessingState)] = {
    case downloadRequestTuple@(_: HttpRequest, downloadUrlToFile: DownloadParams) =>
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

  private def cacheFileForTarEntry(tarEntry: TarArchiveEntry): Graph[FlowShape[ByteString, ByteString], NotUsed] = {
    CacheFile.flow(filename = rootDir.resolve(Paths.get(tarEntry.getName)),
      expectedSize = tarEntry.getSize,
      saveFile = true,
      useSelfDeletingTempFile = true)
  }

  private def metadataFileSource(metadataFile: MetadataFile): Source[ByteString, NotUsed] = {
    httpGetDataSource(metadataFile.uri, metadataFile.tarGzFile.size)
  }

  private def metadataFileCacheFlow(metadataFileDetails: MetadataFileDetails): Graph[FlowShape[ByteString, ByteString], NotUsed] = {
    CacheFile.flow(filename = rootDir.resolve(Paths.get(metadataFileDetails.name)),
      expectedSize = metadataFileDetails.size,
      expectedMd5 = Some(dehexify(metadataFileDetails.md5sum)),
      saveFile = saveTarBalls,
      useSelfDeletingTempFile = true)
  }

  private def httpGetDataSource(uri: String, fileSize: Long): Source[ByteString, NotUsed] = {
    Source.single((HttpRequest(uri = uri), uri))
      .via(Http().superPool())
      .flatMapConcat({
        case (Success(httpRepsonse), _) =>
          httpRepsonse.entity.withSizeLimit(fileSize).dataBytes
        case (Failure(exception), _) =>
          Source.failed(exception)
      })
  }

  def makeRequest(line: Map[String, ByteString]) = HttpRequest(uri = getDownloadUrlFromLine(line))

  private def getDownloadUrlFromLine(line: Map[String, ByteString]) = {
    if (download300K && line("Thumbnail300KURL").nonEmpty) line("Thumbnail300KURL").utf8String
    else line("OriginalURL").utf8String
  }

  def makeDownloadUrlToFile(line: Map[String, ByteString], outputDir: Path, checkMd5IfExists: Boolean): DownloadParams = {
    val originalURLStr: String = getDownloadUrlFromLine(line)
    val originalURL: Uri = Uri(originalURLStr)
    val fileName = originalURL.path.toString().split('/').last
    val fileDirName = fileName.substring(0, 3) // the first 3 chars will create 1000 dirs with 9000 files in each. That's pretty balanced.
    val destPath: Path = rootDir.resolve(outputDir).resolve(fileDirName).resolve(fileName)
    DownloadParams(originalURLStr, destPath, line("OriginalSize").utf8String.toLong,
      line("OriginalMD5").utf8String, checkMd5IfExists, line)
  }

  def outputDir(line: Map[String, ByteString]): java.nio.file.Path = Paths.get(datasetMetadata.topDir, line("Subset").utf8String, originalImagesSubdirectory)

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
                log.debug("checked md5sum of {}. got {}, expected {}",
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
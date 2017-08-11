package nuffer.oidl

import java.nio.file.{Files, Path, Paths}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.stream._
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{Compression, FileIO, Keep, Sink, Source, StreamConverters}
import akka.util.ByteString
import nuffer.oidl.Utils._
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}


case class StartProcessing()

class InputCsvProcessor(downloaderActor: ActorRef, terminatorActor: ActorRef) extends Actor
  with ActorLogging {

  final implicit val system: ActorSystem = context.system
  final implicit val materializer: ActorMaterializer = ActorMaterializer()

  def makeRequest(line: Map[String, ByteString]) = HttpRequest(uri = line("OriginalURL").utf8String)

  def makeDownloadUrlToFile(line: Map[String, ByteString], outputDir: Path, checkMd5IfExists: Boolean): DownloadUrlToFile = {
    val originalURLStr: String = line("OriginalURL").utf8String
    val originalURL: Uri = Uri(originalURLStr)
    val destPath: Path = outputDir.resolve(originalURL.path.toString().split('/').last)
    DownloadUrlToFile(originalURLStr, destPath, line("OriginalSize").utf8String.toLong,
      line("OriginalMD5").utf8String, checkMd5IfExists)
  }

  private def needToDownload(filePath: Path, expectedSize: Long, doCheckMd5: Boolean, expectedMd5: String, alwaysDownload: Boolean): Future[Boolean] = {
    if (alwaysDownload) {
      Future(true)
    } else {
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
  }

  override def receive: Receive = {
    case StartProcessing() =>
      //      log.info("start {}", inputCSVFilename)
      Await.result(
        Source.fromFuture(Http().singleRequest(HttpRequest(uri = "https://storage.googleapis.com/openimages/2017_07/images_2017_07.tar.gz")))
          .flatMapConcat(httpResponse => httpResponse.entity.withoutSizeLimit().dataBytes)
          .via(CacheFile.flow(filename = Paths.get("images_2017_07.tar.gz"),
            expectedSize = 1038132176L,
            expectedMd5 = Some(dehexify("9c7d7d1b9c19f72c77ac0fa8e2695e00")),
            saveFile = true))
          .via(Compression.gunzip())
          .via(CacheFile.flow(filename = Paths.get("images_2017_07.tar"),
            expectedSize = 3362037760L,
            expectedMd5 = Some(dehexify("bff4cdd922f018f343e53e2ffea909f2")),
            saveFile = true))
          .toMat(StreamConverters.asInputStream())(Keep.right)
          .mapMaterializedValue(tarInputStream => {
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
          })
          .run()
          //          .log("tar entry").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
          .map({
            case (tarArchiveInputStream, tarEntry) =>
              log.info("tarEntry.name: {}", tarEntry.getName)
              println(tarEntry.getName)
              Paths.get(tarEntry.getName).getParent.toFile.mkdirs()
              StreamConverters.fromInputStream(() => TarEntryInputStream(tarArchiveInputStream))
                .via(CacheFile.flow(filename = Paths.get(tarEntry.getName), expectedSize = tarEntry.getSize))
                .via(CsvParsing.lineScanner(CsvParsing.Comma, CsvParsing.DoubleQuote, '\0'))
                .via(CsvToMap.toMap())
          })
          .flatMapConcat(identity)
          //          .log("csv line").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
          .fold(Map[String, Int]().withDefaultValue(0)) { (map, csvLine) =>
          map.updated(csvLine("Subset").utf8String, map(csvLine("Subset").utf8String) + 1)
        }
          .log("csv counts").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
          .runWith(Sink.ignore)

        , Duration.Inf)

      terminatorActor ! FatalError

    //      val countCsvEntriesInTar: Boolean = false
    //      if (countCsvEntriesInTar) {
    //        val tais = new TarArchiveInputStream(new FileInputStream("images_2017_07.tar"))
    //
    //        val tarEntrySource = Source.unfold(tais) {
    //          tarArchiveInputStream =>
    //            val nextTarEntry: TarArchiveEntry = tarArchiveInputStream.getNextTarEntry
    //            if (nextTarEntry != null)
    //              if (nextTarEntry.isCheckSumOK) {
    //                Some((tarArchiveInputStream, nextTarEntry))
    //              } else {
    //                throw new TarFileCorruptedException
    //              }
    //            else
    //              None
    //        }
    //
    //        val foo: Source[Source[ByteString, Future[IOResult]], NotUsed] = tarEntrySource.map(tarEntry => {
    //          log.info("tarEntry: {}", tarEntry.getName)
    //          println(tarEntry.getName)
    //          Paths.get(tarEntry.getName).toFile.mkdirs()
    //          StreamConverters.fromInputStream(() => TarEntryInputStream(tais))
    //        })
    //
    //        val bar = foo
    //          .log("tar entry").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
    //          .flatMapConcat(y => y)
    //
    //        val baz = bar
    //          .via(CsvParsing.lineScanner(CsvParsing.Comma, CsvParsing.DoubleQuote, '\0'))
    //          .via(CsvToMap.toMap())
    //          .filterNot(line => line("Subset").utf8String == "Subset")
    //          .fold(Map[String, Int]().withDefaultValue(0)) { (x, y) =>
    //            x.updated(y("Subset").utf8String, x(y("Subset").utf8String) + 1)
    //          }
    //          .log("csv counts").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
    //
    //
    //        Await.result(baz.runWith(Sink.ignore), Duration.Inf)
    //
    //      }

    //      val countLinesInImagesTarGz = false
    //
    //      if (countLinesInImagesTarGz) {
    //        val readFromUrl = false
    //
    //        val tarInputStream =
    //          if (readFromUrl) {
    //            Source.fromFuture(
    //              Http().singleRequest(HttpRequest(uri = "https://storage.googleapis.com/openimages/2017_07/images_2017_07.tar.gz")))
    //              .flatMapConcat(httpResponse => httpResponse.entity.withoutSizeLimit().dataBytes)
    //              .via(Compression.gunzip())
    //              .runWith(StreamConverters.asInputStream())
    //          } else {
    //            new FileInputStream("images_2017_07.tar")
    //          }
    //
    //        val tarArchiveInputStream = new TarArchiveInputStream(tarInputStream)
    //
    //        Await.result(
    //          Source.unfold(tarArchiveInputStream) {
    //            tarArchiveInputStream =>
    //              val nextTarEntry: TarArchiveEntry = tarArchiveInputStream.getNextTarEntry
    //              if (nextTarEntry != null)
    //                if (nextTarEntry.isCheckSumOK) {
    //                  Some((tarArchiveInputStream, nextTarEntry))
    //                } else {
    //                  throw new TarFileCorruptedException
    //                }
    //              else
    //                None
    //          }
    //            .log("tar entry").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
    //            .map(tarEntry => {
    //              log.info("tarEntry: {}", tarEntry.getName)
    //              println(tarEntry.getName)
    //              Paths.get(tarEntry.getName).toFile.mkdirs()
    //              StreamConverters.fromInputStream(() => TarEntryInputStream(tarArchiveInputStream))
    //                .via(CsvParsing.lineScanner(CsvParsing.Comma, CsvParsing.DoubleQuote, '\0'))
    //                .via(CsvToMap.toMap())
    //            })
    //            .flatMapConcat(identity)
    //            //          .log("csv line").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
    //            .fold(Map[String, Int]().withDefaultValue(0)) { (map, csvLine) =>
    //            map.updated(csvLine("Subset").utf8String, map(csvLine("Subset").utf8String) + 1)
    //          }
    //            .log("csv counts").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
    //            .runWith(Sink.ignore),
    //          Duration.Inf)
    //
    //      }

    //      terminatorActor ! FatalError


    //      val downloadImages: Boolean = true
    //
    //      if (downloadImages) {
    //        FileIO.fromPath(inputCSVFilename)
    //          .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Stop))
    //          .via(CsvParsing.lineScanner(CsvParsing.Comma, CsvParsing.DoubleQuote, '\0'))
    //          .via(CsvToMap.toMap())
    //          .map(line => (makeRequest(line), makeDownloadUrlToFile(line, outputDir, checkMd5IfExists)))
    //          .mapAsyncUnordered(Runtime.getRuntime.availableProcessors() * 2)(x =>
    //            for (nd <- needToDownload(x._2.filePath, x._2.expectedSize, x._2.checkMd5IfExists, x._2.expectedMd5, alwaysDownload))
    //              yield (nd, x)
    //          )
    //          .filter(_._1)
    //          .map(_._2)
    //          //        .log("pre http").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
    //          .alsoTo(Sink.foreach(_ => terminatorActor ! StartDownload))
    //          .via(Http().superPool())
    //          .watchTermination() {
    //            case (_, eventualDone) =>
    //              eventualDone.onComplete({
    //                case Failure(error) =>
    //                  log.error("Failed: {}", error)
    //                  System.err.println("Failed: " + error)
    //                  terminatorActor ! FatalError
    //                case Success(_) =>
    //                  log.info("done processing csv")
    //                  context.stop(self)
    //              })
    //          }
    //          .to(Sink.actorRef(downloaderActor, InputCsvProcessingEnd))
    //          .run()
    //      }
  }

  class TarFileCorruptedException extends RuntimeException

}
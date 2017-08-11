package nuffer.oidl

import java.nio.file.{Path, Paths}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.stream._
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{Compression, Keep, Sink, Source, StreamConverters}
import akka.util.ByteString
import nuffer.oidl.Utils._
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Success


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

  override def receive: Receive = {
    case StartProcessing() =>
      Await.result(
        Source.fromFuture(Http().singleRequest(HttpRequest(uri = "https://storage.googleapis.com/openimages/2017_07/images_2017_07.tar.gz")))
          .flatMapConcat(httpResponse => httpResponse.entity.withoutSizeLimit().dataBytes)
          .via(CacheFile.flow(filename = Paths.get("images_2017_07.tar.gz"),
            expectedSize = 1038132176L,
            expectedMd5 = Some(dehexify("9c7d7d1b9c19f72c77ac0fa8e2695e00")),
            saveFile = true,
            useSelfDeletingTempFile = true))
          .via(Compression.gunzip())
          .via(CacheFile.flow(filename = Paths.get("images_2017_07.tar"),
            expectedSize = 3362037760L,
            expectedMd5 = Some(dehexify("bff4cdd922f018f343e53e2ffea909f2")),
            saveFile = true,
            useSelfDeletingTempFile = true))
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
          .map({
            case (tarArchiveInputStream, tarEntry) =>
              log.info("tarEntry.name: {}", tarEntry.getName)
              println(tarEntry.getName)
              Paths.get(tarEntry.getName).getParent.toFile.mkdirs()
              StreamConverters.fromInputStream(() => TarEntryInputStream(tarArchiveInputStream))
                .via(CacheFile.flow(filename = Paths.get(tarEntry.getName),
                  expectedSize = tarEntry.getSize,
                  saveFile = true,
                  useSelfDeletingTempFile = true))
                .via(CsvParsing.lineScanner(CsvParsing.Comma, CsvParsing.DoubleQuote, '\0'))
                .via(CsvToMap.toMap())
          })
          .flatMapConcat(identity)
          .alsoTo(Sink.fold(Map[String, Int]().withDefaultValue(0))({
              (map: Map[String, Int], csvLine: Map[String, ByteString]) => map.updated(csvLine("Subset").utf8String, map(csvLine("Subset").utf8String) + 1)
            }).mapMaterializedValue(futureMap => futureMap.onComplete({
              case Success(m) => log.info("number of lines per type: {}", m)
            }))
          )
          .runWith(Sink.ignore)

        , Duration.Inf)

      terminatorActor ! FatalError




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
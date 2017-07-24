package nuffer.oidl

import java.nio.charset.StandardCharsets
import java.nio.file.Path

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.http.scaladsl.model.Uri
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.FileIO
import akka.stream.{ActorMaterializer, ThrottleMode}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


case class StartProcessingInputCsv(inputCSVFilename: Path, outputDir: Path, checkMd5IfExists: Boolean)

class InputCsvProcessor(downloaderActor: ActorRef, terminatorActor: ActorRef, actorMaterializer: ActorMaterializer) extends Actor
  with ActorLogging {

  final implicit val materializer: ActorMaterializer = actorMaterializer

  override def receive: Receive = {
    case StartProcessingInputCsv(inputCSVFilename: Path, outputDir: Path, checkMd5IfExists: Boolean) =>
      log.info("start {}", inputCSVFilename)

      FileIO.fromPath(inputCSVFilename)
        .throttle(10, 1 seconds, 1, ThrottleMode.Shaping)
        .via(CsvParsing.lineScanner(CsvParsing.Comma, CsvParsing.DoubleQuote, '\0'))
        .limit(1000000)
        .via(CsvToMap.toMap(StandardCharsets.UTF_8))
        .runForeach(line => {
          val originalURLStr: String = line("OriginalURL").utf8String
          val originalURL: Uri = Uri(originalURLStr)
          val destPath: Path = outputDir.resolve(originalURL.path.toString().split('/').last)
          terminatorActor ! StartDownload
          downloaderActor ! DownloadUrlToFile(originalURLStr, destPath, line("OriginalSize").utf8String.toLong,
            line("OriginalMD5").utf8String, checkMd5IfExists)
        }).onComplete({ _ =>
        log.info("done processing csv")
        terminatorActor ! InputCsvProcessingEnd
      })
  }
}

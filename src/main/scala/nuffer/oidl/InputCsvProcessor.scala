package nuffer.oidl

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, IOResult, Supervision}
import akka.util.ByteString
import nuffer.oidl.Utils._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


case class StartProcessingInputCsv(inputCSVFilename: Path, outputDir: Path, checkMd5IfExists: Boolean)

class InputCsvProcessor(downloaderActor: ActorRef, terminatorActor: ActorRef, actorMaterializer: ActorMaterializer) extends Actor
  with ActorLogging {

  final implicit val materializer: ActorMaterializer = actorMaterializer
  final implicit val system: ActorSystem = context.system

  def makeRequest(line: Map[String, ByteString]) = HttpRequest(uri = line("OriginalURL").utf8String)

  def makeDownloadUrlToFile(line: Map[String, ByteString], outputDir: Path, checkMd5IfExists: Boolean): DownloadUrlToFile = {
    val originalURLStr: String = line("OriginalURL").utf8String
    val originalURL: Uri = Uri(originalURLStr)
    val destPath: Path = outputDir.resolve(originalURL.path.toString().split('/').last)
    DownloadUrlToFile(originalURLStr, destPath, line("OriginalSize").utf8String.toLong,
      line("OriginalMD5").utf8String, checkMd5IfExists)
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

  override def receive: Receive = {
    case StartProcessingInputCsv(inputCSVFilename: Path, outputDir: Path, checkMd5IfExists: Boolean) =>
      log.info("start {}", inputCSVFilename)

      FileIO.fromPath(inputCSVFilename)
        .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Stop))
        .via(CsvParsing.lineScanner(CsvParsing.Comma, CsvParsing.DoubleQuote, '\0'))
        .via(CsvToMap.toMap(StandardCharsets.UTF_8))
        .map(line => (makeRequest(line), makeDownloadUrlToFile(line, outputDir, checkMd5IfExists)))
        .mapAsyncUnordered(Runtime.getRuntime.availableProcessors() * 2)(x =>
          for (nd <- needToDownload(x._2.filePath, x._2.expectedSize, x._2.checkMd5IfExists, x._2.expectedMd5))
            yield (nd, x)
        )
        .filter(_._1)
        .map(_._2)
        //        .log("pre http").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
        .alsoTo(Sink.foreach(_ => terminatorActor ! StartDownload))
        .via(Http().superPool())
        .watchTermination() {
          case (_, eventualDone) =>
            eventualDone.onComplete({
              case Failure(error) =>
                log.error("Failed: {}", error)
                System.err.println("Failed: " + error)
                terminatorActor ! FatalError
              case Success(_) =>
                log.info("done processing csv")
                context.stop(self)
              })
        }
        .to(Sink.actorRef(downloaderActor, InputCsvProcessingEnd))
        .run()
  }
}
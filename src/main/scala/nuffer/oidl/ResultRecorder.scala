package nuffer.oidl

import java.nio.file.StandardOpenOption.{APPEND, CREATE, TRUNCATE_EXISTING, WRITE}
import java.nio.file.{Files, Path}

import akka.actor.{Actor, ActorLogging, ActorSystem}
import akka.stream.alpakka.csv.scaladsl.CsvFormatting
import akka.stream.scaladsl.{FileIO, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.util.ByteString
import nuffer.oidl.ResultRecorder.SuccessfulDownload
import resource._

import scala.util.Properties

object ResultRecorder {

  case class SuccessfulDownload()

  case class FailedDownload()

}

class ResultRecorder(doTrain: Boolean,
                     trainSuccess: Path,
                     trainFailure: Path,
                     doValidation: Boolean,
                     validationSuccess: Path,
                     validationFailure: Path,
                     doTest: Boolean,
                     testSuccess: Path,
                     testFailure: Path
                    ) extends Actor
  with ActorLogging {

  final implicit val system: ActorSystem = context.system
  final implicit val materializer: ActorMaterializer = ActorMaterializer()

  val successHeaderLine: String = "ImageID,Subset,OriginalURL,OriginalLandingURL,License,AuthorProfileURL,Author,Title,OriginalSize,OriginalMD5,Thumbnail300KURL"
  val successHeaders: List[String] = successHeaderLine.split(',').toList
  val failureHeaderLine: String = "ImageID,Subset,OriginalURL,OriginalLandingURL,License,AuthorProfileURL,Author,Title,OriginalSize,OriginalMD5,Thumbnail300KURL,FailureMessage"
  val failureHeaders: List[String] = failureHeaderLine.split(',').toList

  def fileDoesntStartWithHeaderLine(filename: Path, headerLine: String): Boolean = {
    managed(Files.lines(filename)) acquireAndGet {
      linesStream => linesStream.limit(1).filter { line => line != headerLine }.findFirst().isPresent
    }
  }

  def writeHeaderIfNecessary(csvFilename: Path, headerLine: String): Unit = {
    if (Files.notExists(csvFilename) || fileDoesntStartWithHeaderLine(csvFilename, headerLine)) {
      for (writer <- managed(Files.newBufferedWriter(csvFilename, WRITE, TRUNCATE_EXISTING, CREATE))) {
        writer.write(headerLine)
        writer.newLine()
      }
    }
  }

  private def makeCsvWriterQueue(outputFilename: Path): SourceQueueWithComplete[Map[String, ByteString]] = {
    Source.queue(0, OverflowStrategy.backpressure)
      .map((csvMap: Map[String, ByteString]) => successHeaders.map { header: String => csvMap(header).utf8String })
      .via(CsvFormatting.format(escapeChar = '\0', endOfLine = Properties.lineSeparator))
      .to(FileIO.toPath(outputFilename, Set(WRITE, APPEND, CREATE)))
      .run()
  }

  if (doTrain) {
    writeHeaderIfNecessary(trainSuccess, successHeaderLine)
    writeHeaderIfNecessary(trainFailure, failureHeaderLine)
  }
  if (doValidation) {
    writeHeaderIfNecessary(validationSuccess, successHeaderLine)
    writeHeaderIfNecessary(validationFailure, failureHeaderLine)
  }
  if (doTest) {
    writeHeaderIfNecessary(testSuccess, successHeaderLine)
    writeHeaderIfNecessary(testFailure, failureHeaderLine)
  }

  val trainSuccessQueue: Option[SourceQueueWithComplete[Map[String, ByteString]]] = if (doTrain) Some(makeCsvWriterQueue(trainSuccess)) else None
  val trainFailureQueue: Option[SourceQueueWithComplete[Map[String, ByteString]]] = if (doTrain) Some(makeCsvWriterQueue(trainFailure)) else None
  val validationSuccessQueue: Option[SourceQueueWithComplete[Map[String, ByteString]]] = if (doValidation) Some(makeCsvWriterQueue(validationSuccess)) else None
  val validationFailureQueue: Option[SourceQueueWithComplete[Map[String, ByteString]]] = if (doValidation) Some(makeCsvWriterQueue(validationFailure)) else None
  val testSuccessQueue: Option[SourceQueueWithComplete[Map[String, ByteString]]] = if (doTest) Some(makeCsvWriterQueue(testSuccess)) else None
  val testFailureQueue: Option[SourceQueueWithComplete[Map[String, ByteString]]] = if (doTest) Some(makeCsvWriterQueue(testFailure)) else None


  override def receive: Receive = {
    case SuccessfulDownload() =>

    case message =>
      log.error("ResultRecorder: Unhandled message: {}", message)
  }
}


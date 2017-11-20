package nuffer.oidl

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.layout.TTLLLayout
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.encoder.LayoutWrappingEncoder
import ch.qos.logback.core.{ConsoleAppender, FileAppender}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.JavaConverters._
import scala.language.postfixOps

object Main extends App {
  def isPowerOfTwo(x: Long) = (x & (x - 1)) == 0

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    version("open_images_downloader 3.0 by Dan Nuffer")
    banner(
      """Usage: open_images_downloader[.bat] [OPTION]...
        |
        |Options:
        |""".stripMargin)
    val rootDir: ScallopOption[String] = opt[String](default = Some("."), noshort = true,
      descr = "top-level directory for storing the Open Images dataset. Default is . (current working directory)")
    //    val originalImagesDir: ScallopOption[String] = opt[String](descr = "If specified, the downloaded original images will be stored in this directory. Otherwise they are placed in <open images dir>/2017_07/{train,validation,test}/images-original")
    val originalImagesSubdirectory: ScallopOption[String] = opt[String](default = Some("images-original"), noshort = true,
      descr = "name of the subdirectory where the original images are stored. Default is images-original")
    val checkMd5IfExists: ScallopOption[Boolean] = toggle(default = Some(true), noshort = true,
      descrYes = "If an image already exists locally in <image dir> and is the same size as the original, check the md5 sum of the file to determine whether to download it. Default is on")
    val alwaysDownload: ScallopOption[Boolean] = toggle(default = Some(false), noshort = true,
      descrYes = "Download and process all images even if the file already exists in <image dir>. This is intended for testing. The check-md5-if-exists option should be sufficient if local data corruption is suspected. Default is off", hidden = true)
    val maxHostConnections: ScallopOption[Long] = opt[Long](default = Some(5), noshort = true,
      descr = "The maximum number of parallel connections to a single host. Default is 5", validate = 0 <)
    // openimages has 16 hostnames in the flickr urls. ($ csvcut -c OriginalURL /tmp/oidl/2017_07/train/images.csv | tail -n+2 | cut -d / -f 3 | sort | uniq | wc -l)
    // so it may be useful to make this 128? Maybe that's a bit excessive?
    val maxTotalConnections: ScallopOption[Long] = opt[Long](default = Some(128), noshort = true,
      descr = "The maximum number of parallel connections to all hosts. Must be a power of 2 and > 0. Default is 128", validate = x => 0 < x && isPowerOfTwo(x))
    val httpPipeliningLimit: ScallopOption[Long] = opt[Long](default = Some(4), noshort = true,
      descr = "The maximum number of parallel pipelined http requests per connection. Default is 4")
    val maxRetries: ScallopOption[Long] = opt[Long](default = Some(15), noshort = true,
      descr = "Number of times to retry failed downloads. Default is 15.", validate = 0 <)
    val logFile: ScallopOption[String] = opt[String](default = None, noshort = true,
      descr = "Write a log to <file>. Default is to not write a log")
    val logToStdout: ScallopOption[Boolean] = toggle(default = Some(true), noshort = true,
      descrYes = "Write the log to stdout. Default is on")
    val saveTarBalls: ScallopOption[Boolean] = toggle(default = Some(false), noshort = true,
      descrYes = "Save the downloaded .tar.gz and .tar files. This uses more space but can save time when resuming from an interrupted execution. Default is off")
    val downloadMetadata: ScallopOption[Boolean] = toggle(default = Some(true), noshort = true,
      descrYes = "Download and extract the metadata files (annotations and classes). Default is on")
    val downloadImages: ScallopOption[Boolean] = toggle(default = Some(true), noshort = true,
      descrYes = "Download and extract the version's image csv file (e.g. images_2017_11.tar.gz) and all images. Default is on")
    val download300K: ScallopOption[Boolean] = toggle(name = "download-300k", default = Some(false), noshort = true,
      descrYes = "Download the image from the url in the Thumbnail300KURL field. This disables verifying the size and md5 hash and results in lower quality images, but may be much faster and use less bandwidth and storage space. These are resized to a max dim of 640, so if you use --resize-mode=ShrinkToFit and --resize-box-size=640 you can get a full consistently sized set of images. For the few images that don't have a 300K url the original is downloaded and needs to be resized. Default is off")
    val saveOriginalImages: ScallopOption[Boolean] = toggle(default = Some(false), noshort = true,
      descrYes = "Save full-size original images. This will use over 18 TB of space. Default is off")
    val resizeImages: ScallopOption[Boolean] = toggle(default = Some(true), noshort = true,
      descrYes = "Resize images. Default is on")
    val resizedImagesSubdirectory: ScallopOption[String] = opt[String](default = Some("images-resized"), noshort = true,
      descr = "name of the subdirectory where the resized images are stored. Default is images-resized")
    val resizeMode: ScallopOption[String] = opt[String](default = Some("ShrinkToFit"), noshort = true,
      descr = "One of ShrinkToFit, FillCrop, or FillDistort. ShrinkToFit will resize images larger than the specified size of bounding box, preserving aspect ratio. Smaller images are unchanged. FillCrop will fill the bounding box, by first either shrinking or growing the image and then doing a center-crop on the larger dimension. FillDistort will fill the bounding box, by either shrinking or growing the image, modifying the aspect ratio as necessary to fit. Default is ShrinkToFit", validate = (opt) => opt == "ShrinkToFit" || opt == "FillCrop" || opt == "FillDistort")
    val resizeBoxSize: ScallopOption[Long] = opt[Long](name = "resize-box-size", default = Some(224), noshort = true,
      descr = "The number of pixels used by resizing for the side of the bounding box. Default is 224")
    val resizeOutputFormat: ScallopOption[String] = opt[String](default = Some("jpg"), noshort = true,
      descr = "The format (and extension) to use for the resized images. Valid values are those supported by ImageMagick. See https://www.imagemagick.org/script/formats.php and/or run identify -list format. Default is jpg")
    val resizeCompressionQuality: ScallopOption[Long] = opt[Long](default = None, noshort = true,
      descr = "The compression quality. If specified, it will be passed with the -quality option to imagemagick convert. See https://www.imagemagick.org/script/command-line-options.php#quality for the meaning of different values and defaults for various output formats. If unspecified, -quality will not be passed and imagemagick will use its default")
    val datasetVersion: ScallopOption[Long] = opt[Long](default = Some(3), noshort = true,
      descr = "The version of the dataset to download. 1, 2, or 3. 3 was released 2017-11-16, 2 was released 2017-07-20, and 1 was released 2016-09-28. Default is 3.")
    verify()
  }

  val conf = new Conf(args)

  configureLogging(conf.logFile, conf.logToStdout)

  private def configureLogging(logFileOpt: ScallopOption[String], logToStdoutOpt: ScallopOption[Boolean]): Unit = {
    val logToStdout = logToStdoutOpt.getOrElse(false)
    val loggerContext = org.slf4j.LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    loggerContext.reset()
    if (logFileOpt.isDefined || logToStdout) {

      val encoder: LayoutWrappingEncoder[ILoggingEvent] = new LayoutWrappingEncoder[ILoggingEvent]()
      encoder.setContext(loggerContext)

      // same as
      // PatternLayout layout = new PatternLayout();
      // layout.setPattern("%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n");
      val layout = new TTLLLayout

      layout.setContext(loggerContext)
      layout.start()
      encoder.setLayout(layout)

      loggerContext.getLoggerList
      val rootLogger = loggerContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
      if (logFileOpt.isDefined) {
        val fileAppender = new FileAppender[ILoggingEvent]()
        fileAppender.setFile(logFileOpt())
        fileAppender.setContext(loggerContext)
        fileAppender.setName("file")
        fileAppender.setEncoder(encoder)
        fileAppender.start()
        rootLogger.addAppender(fileAppender)
      }
      if (logToStdout) {
        val consoleAppender = new ConsoleAppender[ILoggingEvent]()
        consoleAppender.setTarget("System.out")
        consoleAppender.setContext(loggerContext)
        consoleAppender.setName("console")
        consoleAppender.setEncoder(encoder)
        consoleAppender.start()
        rootLogger.addAppender(consoleAppender)
      }

    }
  }

  val mainConfig = ConfigFactory.load()
    .withValue("akka.http.client.connecting-timeout", ConfigValueFactory.fromAnyRef("1 min"))
    .withValue("akka.http.client.idle-timeout", ConfigValueFactory.fromAnyRef("infinite"))
    .withValue("akka.http.host-connection-pool.max-connections", ConfigValueFactory.fromAnyRef(conf.maxHostConnections()))
    .withValue("akka.http.host-connection-pool.max-retries", ConfigValueFactory.fromAnyRef(conf.maxRetries()))
    .withValue("akka.http.host-connection-pool.max-open-requests", ConfigValueFactory.fromAnyRef(conf.maxTotalConnections()))
    .withValue("akka.http.host-connection-pool.pipelining-limit", ConfigValueFactory.fromAnyRef(1))
    .withValue("akka.http.host-connection-pool.idle-timeout", ConfigValueFactory.fromAnyRef("infinite"))
    .withValue("akka.http.host-connection-pool.client.connecting-timeout", ConfigValueFactory.fromAnyRef("1 min"))
    .withValue("akka.http.host-connection-pool.client.idle-timeout", ConfigValueFactory.fromAnyRef("infinite"))
    .withValue("akka.stdout-loglevel", ConfigValueFactory.fromAnyRef("ERROR"))
    .withValue("akka.loggers", ConfigValueFactory.fromIterable(List("akka.event.slf4j.Slf4jLogger").asJava))
    .withValue("akka.loglevel", ConfigValueFactory.fromAnyRef("INFO"))
    .withValue("akka.logging-filter", ConfigValueFactory.fromAnyRef("akka.event.slf4j.Slf4jLoggingFilter"))


  implicit val system = ActorSystem("oidl", mainConfig)
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  val http = Http(system)
  val log = Logging(system, this.getClass)

  val director = Director(
    Paths.get(conf.rootDir.getOrElse(".")),
    conf.originalImagesSubdirectory(),
    conf.checkMd5IfExists(),
    conf.alwaysDownload(),
    conf.saveTarBalls(),
    conf.downloadMetadata(),
    conf.downloadImages(),
    conf.download300K(),
    conf.saveOriginalImages(),
    conf.resizeImages(),
    conf.resizedImagesSubdirectory(),
    ResizeMode.withName(conf.resizeMode()),
    conf.resizeBoxSize(),
    conf.resizeOutputFormat(),
    conf.resizeCompressionQuality.toOption,
    conf.datasetVersion())

  director.run().onComplete({
    _ =>
      Http().shutdownAllConnectionPools().onComplete({ _ =>
        log.info("context.system.terminate()")
        system.terminate()
      })
  })
}

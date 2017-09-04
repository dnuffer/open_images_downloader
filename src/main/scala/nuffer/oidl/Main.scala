package nuffer.oidl

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.layout.TTLLLayout
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.FileAppender
import ch.qos.logback.core.encoder.LayoutWrappingEncoder
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.JavaConverters._

object Main extends App {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val rootDir: ScallopOption[String] = opt[String](default = Some("/tmp/oidl"), descr = "top-level directory for storing the Open Images dataset")
    //    val originalImagesDir: ScallopOption[String] = opt[String](descr = "If specified, the downloaded original images will be stored in this directory. Otherwise they are placed in <open images dir>/2017_07/{train,validation,test}/images-original")
    val checkMd5IfExists: ScallopOption[Boolean] = toggle(default = Some(true), descrYes = "If an image already exists locally in <image dir> and is the same size as the original, check the md5 sum of the file to determine whether to download it.")
    val alwaysDownload: ScallopOption[Boolean] = toggle(default = Some(false), descrYes = "Download and process all images even if the file already exists in <image dir>. This is intended for testing. The check-md5-if-exists option should be sufficient if local data corruption is suspected.")
    val maxRetries: ScallopOption[Int] = opt[Int](default = Some(15), descr = "Number of times to retry failed downloads", validate = 0 <)
    val logFile: ScallopOption[String] = opt[String](default = Some("-"), descr = "Write a log to <file>. Specify - for stdout")
    val saveTarBalls: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Save the downloaded .tar.gz and .tar files. This uses more space but can save time when resuming from an interrupted execution.")
    val downloadMetadata: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Download and extract the metadata files (annotations and classes)")
    val downloadImages: ScallopOption[Boolean] = opt[Boolean](default = Some(true), descr = "Download and extract images_2017_07.tar.gz and all images")
    val saveOriginalImages: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Save full-size original images. This will use over 10 TB of space.")
    val resizeImages: ScallopOption[Boolean] = opt[Boolean](default = Some(true), descr = "Resize images.")
    verify()
  }

  val conf = new Conf(args)

  if (conf.logFile() != "-") {
    configureLoggingToFile(conf.logFile)
  }


  private def configureLoggingToFile(logFileOpt: ScallopOption[String]): Unit = {
    val lc = org.slf4j.LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    lc.reset()
    if (logFileOpt.isDefined) {
      val fa = new FileAppender[ILoggingEvent]()
      fa.setFile(logFileOpt())
      fa.setContext(lc)
      fa.setName("file")

      val encoder: LayoutWrappingEncoder[ILoggingEvent] = new LayoutWrappingEncoder[ILoggingEvent]()
      encoder.setContext(lc)

      // same as
      // PatternLayout layout = new PatternLayout();
      // layout.setPattern("%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n");
      val layout = new TTLLLayout

      layout.setContext(lc)
      layout.start()
      encoder.setLayout(layout)

      fa.setEncoder(encoder)
      fa.start()

      lc.getLoggerList
      val rootLogger = lc.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
      rootLogger.addAppender(fa)
    }
  }

  val mainConfig = ConfigFactory.load()
    .withValue("akka.http.host-connection-pool.max-retries", ConfigValueFactory.fromAnyRef(conf.maxRetries()))
    .withValue("akka.http.host-connection-pool.idle-timeout", ConfigValueFactory.fromAnyRef("infinite"))
    .withValue("akka.http.client.idle-timeout", ConfigValueFactory.fromAnyRef("infinite"))
    .withValue("akka.http.host-connection-pool.client.idle-timeout", ConfigValueFactory.fromAnyRef("infinite"))
    .withValue("akka.stream.default-blocking-io-dispatcher.thread-pool-executor.fixed-pool-size", ConfigValueFactory.fromAnyRef("128"))
    .withValue("akka.actor.default-dispatcher.fork-join-executor.parallelism-factor", ConfigValueFactory.fromAnyRef("4.0"))
    .withValue("akka.stdout-loglevel", ConfigValueFactory.fromAnyRef("ERROR"))
    .withValue("akka.loggers", ConfigValueFactory.fromIterable(List("akka.event.slf4j.Slf4jLogger").asJava))
    .withValue("akka.loglevel", ConfigValueFactory.fromAnyRef("INFO"))
    .withValue("akka.logging-filter", ConfigValueFactory.fromAnyRef("akka.event.slf4j.Slf4jLoggingFilter"))

  //    .withValue("akka.http.host-connection-pool.max-connections", ConfigValueFactory.fromAnyRef(1))
  //    .withValue("akka.http.host-connection-pool.idle-timeout", ConfigValueFactory.fromAnyRef("1 s"))
  //    .withValue("akka.http.host-connection-pool.client.idle-timeout", ConfigValueFactory.fromAnyRef("1ms"))


  implicit val system = ActorSystem("oidl", mainConfig)
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  val http = Http(system)
  val log = Logging(system, this.getClass)

  val director = Director(
    Paths.get(conf.rootDir.getOrElse(".")),
    conf.checkMd5IfExists(),
    conf.alwaysDownload(),
    conf.saveTarBalls(),
    conf.downloadMetadata(),
    conf.downloadImages(),
    conf.saveOriginalImages(),
    conf.resizeImages())

  director.run().onComplete({
    _ =>
      Http().shutdownAllConnectionPools().onComplete({ _ =>
        //        log.info("sleeping")
        //        Thread.sleep(10000)
        log.info("context.system.terminate()")
        system.terminate()
      })
  })
}

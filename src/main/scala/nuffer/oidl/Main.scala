package nuffer.oidl

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}

object Main extends App {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val rootDir: ScallopOption[String] = opt[String](default = Some("/tmp/oidl"), descr = "top-level directory for storing the Open Images dataset")
    val imagesCsv: ScallopOption[String] = opt[String](descr = "If this option is specified, only download and process the images in the indicated file.")
    val originalImagesDir: ScallopOption[String] = opt[String](descr = "If specified, the downloaded original images will be stored in this directory. Otherwise they are placed in <open images dir>/2017_07/{train,validation,test}/images")
    val checkMd5IfExists: ScallopOption[Boolean] = toggle(default = Some(false), descrYes = "If an image already exists locally in <image dir> and is the same size as the original, check the md5 sum of the file to determine whether to download it.")
    val alwaysDownload: ScallopOption[Boolean] = toggle(default = Some(false), descrYes = "Download and process all images even if the file already exists in <image dir>. This is intended for testing. The check-md5-if-exists option should be sufficient if local data corruption is suspected.")
    val doTrain: ScallopOption[Boolean] = toggle(default = Some(true), descrYes = "Download and process images in the training set")
    val doValidation: ScallopOption[Boolean] = toggle(default = Some(true), descrYes = "Download and process images in the validation set")
    val doTest: ScallopOption[Boolean] = toggle(default = Some(true), descrYes = "Download and process images in the test set")
    val maxRetries: ScallopOption[Int] = opt[Int](default = Some(15), descr = "Number of times to retry failed downloads", validate = 0 <)
    verify()
  }

  val conf = new Conf(args)

  val mainConfig = ConfigFactory.load()
    .withValue("akka.http.host-connection-pool.max-retries", ConfigValueFactory.fromAnyRef(conf.maxRetries()))
    .withValue("akka.http.host-connection-pool.idle-timeout", ConfigValueFactory.fromAnyRef("infinite"))
    .withValue("akka.http.client.idle-timeout", ConfigValueFactory.fromAnyRef("infinite"))
    .withValue("akka.http.host-connection-pool.client.idle-timeout", ConfigValueFactory.fromAnyRef("infinite"))
    .withValue("akka.stream.default-blocking-io-dispatcher.thread-pool-executor.fixed-pool-size", ConfigValueFactory.fromAnyRef("128"))
    .withValue("akka.actor.default-dispatcher.fork-join-executor.parallelism-factor", ConfigValueFactory.fromAnyRef("4.0"))
    .withValue("akka.stdout-loglevel", ConfigValueFactory.fromAnyRef("ERROR"))
//    .withValue("akka.loglevel", ConfigValueFactory.fromAnyRef("DEBUG"))
  //    .withValue("akka.http.host-connection-pool.max-connections", ConfigValueFactory.fromAnyRef(1))
  //    .withValue("akka.http.host-connection-pool.idle-timeout", ConfigValueFactory.fromAnyRef("1 s"))
  //    .withValue("akka.http.host-connection-pool.client.idle-timeout", ConfigValueFactory.fromAnyRef("1ms"))
  implicit val system = ActorSystem("oidl", mainConfig)
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  val http = Http(system)
  val log = Logging(system, this.getClass)

  val director = Director(Paths.get(conf.rootDir.getOrElse(".")))
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

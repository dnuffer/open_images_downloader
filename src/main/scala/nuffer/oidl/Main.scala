package nuffer.oidl

import java.nio.file.Paths

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import org.rogach.scallop.{ScallopConf, ScallopOption}

object Main extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  val http = Http(system)


  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val images_csv: ScallopOption[String] = opt[String](default = Some("test10images.csv"))
    val images_dir: ScallopOption[String] = opt[String](default = Some("images"))
    val checkMd5IfExists: ScallopOption[Boolean] = opt[Boolean](default = Some(true))
    verify()
  }

  val conf = new Conf(args)

  val terminatorActor = system.actorOf(Props(classOf[Terminator], materializer, http), "Terminator")
  val downloaderActor = system.actorOf(Props(classOf[Downloader], terminatorActor, materializer, http), name = "Downloader")
  val inputCsvProcessorActor = system.actorOf(Props(classOf[InputCsvProcessor], downloaderActor, terminatorActor, materializer), name = "InputCsvProcessor")

  inputCsvProcessorActor ! StartProcessingInputCsv(Paths.get(conf.images_csv()), Paths.get(conf.images_dir()), conf.checkMd5IfExists())
}

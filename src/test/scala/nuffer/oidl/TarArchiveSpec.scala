package nuffer.oidl

import java.nio.channels.SeekableByteChannel
import java.nio.file.{Files, Paths, StandardOpenOption}

import akka.NotUsed
import akka.stream.scaladsl.{FileIO, Flow, Keep, RunnableGraph, Sink}
import akka.stream.testkit.StreamSpec
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, IOResult}
import akka.util.ByteString
import org.apache.commons.compress.archivers.tar.TarArchiveEntry

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class TarArchiveSpec extends StreamSpec {
  val settings = ActorMaterializerSettings(system)
    .withInputBuffer(initialSize = 1, maxSize = 1)

  implicit val materializer = ActorMaterializer(settings)

  override def expectedTestDuration: FiniteDuration = 10 minutes

  def extract(tarfileName: String): Unit = {
    val future: Future[Option[SeekableByteChannel]] = FileIO.fromPath(Paths.get(tarfileName))
      .via(TarArchive.flow())
      .runWith(Sink.fold(Option.empty[SeekableByteChannel])({
        case (Some(chan), (tarArchiveEntry: TarArchiveEntry, bytes: ByteString, None)) =>
          chan.write(bytes.toByteBuffer)
          Some(chan)
        case (_, (tarArchiveEntry: TarArchiveEntry, bytes: ByteString, Some(_: TarArchiveEntryFirstRecord))) =>
          println("Got first record for " + tarArchiveEntry.getName)
          val path = Paths.get(tarArchiveEntry.getName)
          Files.createDirectories(path.getParent)
          val chan: SeekableByteChannel = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
          chan.write(bytes.toByteBuffer)
          Some(chan)
      }))
    future
      .onComplete(res => println("Extraction complete: " + res))
    Await.result(future, Duration.Inf)
  }

  def extractViaSubflow(tarfileName: String): Unit = {
    val sinkFactory: ((TarArchiveEntry, ByteString)) => Future[Sink[(TarArchiveEntry, ByteString), Future[IOResult]]] = {
      case (tae: TarArchiveEntry, _: ByteString) =>
        Future[Sink[(TarArchiveEntry, ByteString), Future[IOResult]]] {
          Flow[(TarArchiveEntry, ByteString)]
            .map(_._2)
            .toMat({
              val path = Paths.get(tae.getName)
              Files.createDirectories(path.getParent)
              println("Begin writing to " + path)
              FileIO.toPath(path)
            })(Keep.right)
        }
    }

    val lazyInitSink: Sink[(TarArchiveEntry, ByteString), Future[Future[IOResult]]] = Sink.lazyInit(
      sinkFactory,
      () => Future {
        IOResult.createSuccessful(0)
      }
    )

    val subflow: Sink[ByteString, NotUsed] = TarArchive.subflowPerEntry()
      .to(lazyInitSink)


    val graph: RunnableGraph[Future[IOResult]] = FileIO.fromPath(Paths.get(tarfileName))
      .to(subflow)

    val future: Future[IOResult] = graph
      .run()

    future.onComplete(res =>
      println("Extraction complete: " + res)
    )
    Await.result(future, Duration.Inf)
  }

  "TarArchive" must {
    //    "extract 0 byte files" in {
    //      //      val tempFile: Path = Files.createTempFile("TarArchiveSpec", "")
    //      // run tar to create an archive with the 0-byte temp file.
    //      // Check TarArchive.flow() outputs the correct values.
    //    }

    "extract classes_2017_07.tar" in {
      extractViaSubflow("classes_2017_07.tar")
    }
    "extract annotations_human_2017_07.tar" in {
      extractViaSubflow("annotations_human_2017_07.tar")
    }
    "extract annotations_human_bbox_2017_07.tar" in {
      extractViaSubflow("annotations_human_bbox_2017_07.tar")
    }
    "extract annotations_machine_2017_07.tar" in {
      extractViaSubflow("annotations_machine_2017_07.tar")
    }
    "extract images_2017_07.tar" in {
      extractViaSubflow("images_2017_07.tar")
    }
  }
}

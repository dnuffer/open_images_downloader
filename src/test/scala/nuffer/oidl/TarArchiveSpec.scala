package nuffer.oidl

import java.nio.channels.SeekableByteChannel
import java.nio.file.{Files, Paths, StandardOpenOption}

import akka.stream._
import akka.stream.scaladsl.{FileIO, Flow, GraphDSL, Keep, Sink, Source}
import akka.stream.testkit.StreamSpec
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.apache.commons.compress.archivers.tar.TarArchiveEntry

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class TarArchiveSpec extends StreamSpec {
  val settings = ActorMaterializerSettings(system)
    .withInputBuffer(initialSize = 1, maxSize = 1)

  implicit val materializer = ActorMaterializer(settings)

  override def expectedTestDuration: FiniteDuration = 10 minutes

  def extractUsingStatefulMapConcat(tarfileName: String): Unit = {
    val ioResults: Source[IOResult, Future[IOResult]] = FileIO.fromPath(Paths.get(tarfileName))
      .via(TarArchive.flow())
      .statefulMapConcat(getWriteToFile)

    val future = ioResults.runWith(Sink.foreach(println))

    future
      .onComplete(res => println("Extraction complete: " + res))
    Await.result(future, Duration.Inf)
  }

  private def getWriteToFile: () => ((TarArchiveEntry, ByteString, TarArchiveEntryRecordOrder)) => List[IOResult] = () => {
    var chan: Option[SeekableByteChannel] = None
    var bytesWritten: Long = 0
    (_: (TarArchiveEntry, ByteString, TarArchiveEntryRecordOrder)) match {
      case (tarArchiveEntry: TarArchiveEntry, bytes: ByteString, order: TarArchiveEntryRecordOrder) =>
        if (order.first) {
          println(s"Got first record for ${tarArchiveEntry.getName}")
          val path = Paths.get(tarArchiveEntry.getName)
          if (path.getParent() != null) Files.createDirectories(path.getParent)
          chan = Some(Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))
        }
        try {
          chan.foreach(_.write(bytes.toByteBuffer))
          bytesWritten += bytes.length
          if (order.last) {
            chan = None
            List(IOResult(bytesWritten, Success(Done)))
          } else {
            List()
          }
        }
        catch {
          case NonFatal(exception) => List(IOResult(bytesWritten, Failure(exception)))
        }
    }
  }

  def extractUsingSinkFold(tarfileName: String): Unit = {
    val future: Future[IOResult] = FileIO.fromPath(Paths.get(tarfileName))
      .via(TarArchive.flow())
      .to(Sink.fold(Option.empty[SeekableByteChannel])({
        case (_, (tarArchiveEntry: TarArchiveEntry, bytes: ByteString, TarArchiveEntryRecordOrder(true, _))) =>
          println(s"Got first record for ${tarArchiveEntry.getName}")
          val path = Paths.get(tarArchiveEntry.getName)
          Files.createDirectories(path.getParent)
          val chan: SeekableByteChannel = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
          chan.write(bytes.toByteBuffer)
          Some(chan)

        case (Some(chan), (_: TarArchiveEntry, bytes: ByteString, TarArchiveEntryRecordOrder(false, _))) =>
          chan.write(bytes.toByteBuffer)
          Some(chan)
      }))
      .run()

    future
      .onComplete(res => println("Extraction complete: " + res))
    Await.result(future, Duration.Inf)
  }

  def extractViaSubflow(tarfileName: String): Unit = {
    // note that this method is over 5x as slow as extract()
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

    val lazyInitSink: Sink[(TarArchiveEntry, ByteString), Future[IOResult]] = Sink.lazyInit(
      sinkFactory,
      () => Future {
        IOResult.createSuccessful(0)
      }
    ).mapMaterializedValue(ffRes => ffRes.flatMap(fRes => fRes))

    val toFileFlow: Flow[(TarArchiveEntry, ByteString), Future[IOResult], Future[IOResult]] =
      Flow.fromGraph(GraphDSL.create(lazyInitSink) {
        implicit builder =>
          sink =>
            FlowShape(sink.in, builder.materializedValue)
      })

    val tarExtractionFlow: Flow[ByteString, Future[IOResult], NotUsed] = TarArchive.subflowPerEntry()
      .via(toFileFlow)
      .concatSubstreams

    val future: Future[IOResult] = FileIO.fromPath(Paths.get(tarfileName))
      .via(tarExtractionFlow)
      .to(Sink.foreach(_.onComplete(println)))
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
//      extractUsingStatefulMapConcat("classes_2017_07.tar")
    }
    "extract annotations_human_2017_07.tar" in {
//      extractUsingStatefulMapConcat("annotations_human_2017_07.tar")
    }
    "extract annotations_human_bbox_2017_07.tar" in {
//      extractUsingStatefulMapConcat("annotations_human_bbox_2017_07.tar")
    }
    "extract annotations_machine_2017_07.tar" in {
//      extractUsingStatefulMapConcat("annotations_machine_2017_07.tar")
    }
    "extract images_2017_07.tar" in {
//      extractUsingStatefulMapConcat("images_2017_07.tar")
    }
  }
}

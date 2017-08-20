package nuffer.oidl

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.file.{Files, Paths}
import javax.imageio.ImageIO

import akka.stream._
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.StreamSpec
import akka.util.ByteString
import org.im4java.core.{ConvertCmd, IMOperation}
import org.im4java.process.Pipe

import scala.concurrent.ExecutionContext
//import magick.{ImageInfo, MagickImage}
//import org.libjpegturbo.turbojpeg.{TJCompressor, TJDecompressor}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

class ImageResizeTest extends StreamSpec {

  val settings = ActorMaterializerSettings(system)
    .withInputBuffer(initialSize = 1, maxSize = 1)

  implicit val materializer = ActorMaterializer(settings)

  override def expectedTestDuration: FiniteDuration = 10 minutes

  def time[R](name: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println(name + " elapsed time: " + (t1 - t0) + " ns / " + (t1 - t0) / 1e9 + " s")
    result
  }

  val imagesBytes = time("image loading") {
    Await.result(
      Directory.ls(Paths.get("1000-test-images"))
        .mapAsyncUnordered(8)(path => Future(Files.readAllBytes(path)))
        .runFold(List[Array[Byte]]())((s: List[Array[Byte]], bs: Array[Byte]) => s :+ bs)
        .andThen({
          case Success(_) => println("finished loading")
          case Failure(ex) => println("failed loading: " + ex)
        })
      , Duration.Inf)
  }

  "jpeg decoding" must {
    //    "ImageIO.read" in {
    //      time("ImageIO.read") {
    //        for (imageBytes <- imagesBytes) {
    //          val image: BufferedImage = ImageIO.read(new ByteArrayInputStream(imageBytes.toArray))
    //        }
    //      }
    //      // This can't deal with actual images and throws javax.imageio.IIOException: Incompatible color conversion
    //    }
    "TwelveMonkeys ImageIO" in {
      time("TwelveMonkeys ImageIO.read") {

        for (imageBytes <- imagesBytes) {
          val image = ImageIO.read(new ByteArrayInputStream(imageBytes))
        }
      }
      // TwelveMonkeys ImageIO.read elapsed time: 140453010210 ns / 140.45301021 s
      // That is 114 Mb/s. not fast enough. Maybe with 22 cores it could barely squeak by.
    }

    "libjpeg-turbo" in {
      time("libjpeg-turbo read") {
        //        System.loadLibrary("turbojpeg")

        //        for (imageBytes <- imagesBytes) {
        //          val d = new TJDecompressor(imageBytes)
        //          println(d.getHeight, d.getWidth)
        //        }
      }
      // dan@think:~/extsrc/libjpeg-turbo-1.5.2$ time for x in ~/src/open_images_downloader/2017_07/train/images-original/*.jpg; do djpeg $x > /dev/null; done
      // Corrupt JPEG data: 24 extraneous bytes before marker 0xd9
      // PPM output must be grayscale or RGB
      //
      // real	0m42.871s
      // user	0m40.188s
      // sys	0m0.684s

      // not bad, this was a pessimistic test, including the time to load the files and execute the djpeg program. It is still 30% of the time of TwelveMonkeys. Unfortunately integrating it with java might be a huge pita. The images are 2.0 GB. So this is running at 373 Mb/s, on my laptop.
      // Not fast enough to saturate a gigabit pipe with one core. But running with parallel on 4 cores does get the speed up to 1.48 Gb/s.
      // If I use scale 1/2, it gets even faster: 1.94 Gb/s. On hurley w/22 cores, it runs at 4.7 Gb/s.
    }

    "jmagick" in {
      time("jmagick read") {
        //        val imageInfo = new ImageInfo()
        //        for (imageBytes <- imagesBytes) {
        //          val image = new MagickImage(imageInfo, imageBytes)
        //        }
      }
    }

    "im4java" in {
      time("im4java resize") {
        val convertCmd = new ConvertCmd()
        val op = new IMOperation()
        op.addImage("-"); // read from stdin
        op.addImage("jpg:-"); // write to stdout in jpeg-format
        op.define("jpeg:size=598x598")
        op.autoOrient()
        op.thumbnail(299, 299, '>')
        for (imageBytes <- imagesBytes) {
          println("starting new image")
          val is = new ByteArrayInputStream(imageBytes)
          val os = new ByteArrayOutputStream(4092)
          val pipeIn = new Pipe(is, null)
          val pipeOut = new Pipe(null, os)
          convertCmd.setInputProvider(pipeIn)
          convertCmd.setOutputConsumer(pipeOut)
          convertCmd.run(op)
        }
      }
    }

    "scala process convert" in {
      time("convert resize") {
        for (imageBytes <- imagesBytes.take(10)) {
          //          println("waiting for result")
          val res = Await.result(
            Source.single(ByteString(imageBytes))
              .flatMapConcat(bs => Source.apply(scala.collection.immutable.List(bs)))
              .map(b => ByteString(b: _*))
              .throttle(1000, 1.second, 10, mode = ThrottleMode.Shaping)
              .via(ImageResize.resizeJpgFlow(299))
              .runWith(Sink.reduce((b: ByteString, b2: ByteString) => b.++(b2))),
            Duration.Inf)
          print(".")
          //          println("got result")
        }
      }
    }

    "scala process convert parallel" in {
      time("convert resize") {
        // need an unlimited size thread pool for blocking io threads.
        implicit val ec = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newCachedThreadPool())
        for (imageBytesList <- imagesBytes.grouped(200)) {
          //          println("waiting for result")
          val res = Await.result(
            Future.sequence(imageBytesList.map { imageBytes =>
              Source.single(ByteString(imageBytes))
                .flatMapConcat(bs => Source(scala.collection.immutable.List(bs)))
                .map(b => ByteString(b: _*))
                .throttle(1000, 1.second, 10, mode = ThrottleMode.Shaping)
                .via(ImageResize.resizeJpgFlow(299))
                .runWith(Sink.reduce[ByteString](_ ++ _))
            }),
            Duration.Inf)
          println(res.size, res.map(_.size))
          //          println("got result")
        }
      }
    }
  }


  "image resizing" must {
    // pre-allocate buffers when possible
    "imgscalr" in {

    }

    "Twelve Monkeys ResampleOp (https://haraldk.github.io/TwelveMonkeys/)" in {

    }

    "libjpeg-turbo read and resize" in {

    }

    "Thumbnailator (https://github.com/coobird/thumbnailator)" in {

    }
  }

  "jpeg writing" must {
    "ImageIO.write" in {

    }
  }

}
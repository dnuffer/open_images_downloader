package nuffer.oidl

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

object ImageResize {
  // alternatives:
  // overview: https://stackoverflow.com/questions/1069095/how-do-you-create-a-thumbnail-image-out-of-a-jpeg-in-java
  // use area interpolation for shrinking and bilinear for growing. cv2 INTER_AREA, INTER_LINEAR
  // see http://tanbakuchi.com/posts/comparison-of-openv-interpolation-algorithms/ and
  // http://docs.opencv.org/trunk/da/d6e/tutorial_py_geometric_transformations.html
  // need to try out various library and performance test them.
  // https://github.com/fawick/speedtest-resize has perf tests for golang.
  // image resize - I want a library that does it, and not depend on an external process (e.g. imagemagic)
  // imagemagick thumbnail seems rather fast. Does it have java bindings?
  // libvips is the fastest, but it doesn't have java bindings, so it's out of consideration, but it uses libjpeg-turbo.
  // opencv, with javacv - https://github.com/bytedeco/javacv
  // opencv, with javacv, with sbt-javacv - https://github.com/bytedeco/sbt-javacv
  // opencv, with javacpp-presets - https://github.com/bytedeco/javacpp-presets/tree/master/opencv
  // opencv, with java bindings - https://github.com/openpnp/opencv
  // libjpeg-turbo - http://www.libjpeg-turbo.org/ Has java bindings. Is what tensorflow uses. Claims to be very fast.
  //                 java api doc: https://cdn.rawgit.com/libjpeg-turbo/libjpeg-turbo/dev/java/doc/index.html
  //                 maven repo with it: https://mvnrepository.com/artifact/ome/turbojpeg
  // thumbnailinator
  // apache commons imaging - https://commons.apache.org/proper/commons-imaging/whyimaging.html
  // javax.imageio - fails with javax.imageio.IIOException: Incompatible color conversion
  // imgscalr - https://github.com/rkalla/imgscalr
  //            seems abandoned, but with a good focus on quality and compatibility. Maybe sufficient for now.
  //            only does resize, not load or save.
  // The only easy-to-integrate java library that provides suitable compatibility
  // and quality is TwelveMonkeys (for loading/saving) + imgscalr (for resize), which is much slower than imagemagick.
  // So after all that research, we're using imagemagick convert via command line.

  def resizeJpgFlow(maxDim: Int)(implicit ec: ExecutionContext, materializer: Materializer): Flow[ByteString, ByteString, Future[Done]] =
    ProcessPipe.throughProcessCheckForError(
      s"convert -define jpeg:size=${maxDim * 2}x${maxDim * 2} - -auto-orient -thumbnail ${maxDim}x${maxDim}> jpg:-")
}

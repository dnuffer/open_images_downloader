package nuffer.oidl

import akka.NotUsed
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import org.apache.commons.compress.archivers.tar.TarArchiveEntry

class TarFileCorruptedException extends RuntimeException

object TarArchive {
  def flow(): Graph[FlowShape[ByteString, TarArchiveEntry], NotUsed] = ??? //{

    //    val inputSink: Sink[ByteString, InputStream] = StreamConverters.asInputStream()
    //    inputSink.mapMaterializedValue()
    //    val tarInputStream: InputStream = source.runWith(inputSink)
    //    val tarArchiveInputStream = new TarArchiveInputStream(tarInputStream)
    //    val tarEntriesSource: Source[TarArchiveEntry, NotUsed] = Source.unfold(tarArchiveInputStream) {
    //      tarArchiveInputStream =>
    //        val nextTarEntry: TarArchiveEntry = tarArchiveInputStream.getNextTarEntry
    //        if (nextTarEntry != null)
    //          if (nextTarEntry.isCheckSumOK) {
    //            Some((tarArchiveInputStream, nextTarEntry))
    //          } else {
    //            throw new TarFileCorruptedException
    //          }
    //        else
    //          None
    //    }
    //    Flow.fromSinkAndSource(input, tarEntriesSource)
    //    fromGraph(GraphDSL.create(sink, source)(combine) { implicit b ⇒ (in, out) ⇒ FlowShape(in.in, out.out) })
//  }
}

class TarArchive extends GraphStage[FlowShape[ByteString, TarArchiveEntry]] {
  val in = Inlet[ByteString]("TarArchive.in")
  val out = Outlet[TarArchiveEntry]("TarArchive.out")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    override def onPush(): Unit = ???

    override def onPull(): Unit = ???
  }
}

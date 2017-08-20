package nuffer.oidl

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, StreamConverters}
import akka.util.ByteString
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}

class TarFileCorruptedException extends RuntimeException

object TarArchive {
  def flow(): Graph[FlowShape[ByteString, Source[(TarArchiveInputStream, TarArchiveEntry), NotUsed]], NotUsed] = {
    val tarSink: Sink[ByteString, Source[(TarArchiveInputStream, TarArchiveEntry), NotUsed]] =
      Flow[ByteString].toMat(StreamConverters.asInputStream())(Keep.right)
        .mapMaterializedValue(tarInputStream => {
          val tarArchiveInputStream = new TarArchiveInputStream(tarInputStream)
          Source.unfold(tarArchiveInputStream) {
            tarArchiveInputStream: TarArchiveInputStream =>
              val nextTarEntry: TarArchiveEntry = tarArchiveInputStream.getNextTarEntry
              if (nextTarEntry != null)
                if (nextTarEntry.isCheckSumOK) {
                  Some((tarArchiveInputStream, (tarArchiveInputStream, nextTarEntry)))
                } else {
                  throw new TarFileCorruptedException
                }
              else
                None
          }
        })

    Flow.fromGraph(GraphDSL.create(tarSink) { implicit builder => tarSink2 =>
      val materializedValue: Outlet[Source[(TarArchiveInputStream, TarArchiveEntry), NotUsed]] = builder.materializedValue
      FlowShape(tarSink2.in, materializedValue)
    }).mapMaterializedValue(_ => NotUsed)
  }
}
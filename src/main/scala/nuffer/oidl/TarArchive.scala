package nuffer.oidl

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import org.apache.commons.compress.archivers.tar.TarArchiveEntry

case class TarFileCorruptedException() extends RuntimeException("checksum of header record is invalid")

/**
  * This is used to indicate the first/last record output from the flow for each tar archive entry, which will trigger a new SubFlow for each
  * new file in the tar archive.
  */
case class TarArchiveEntryRecordOrder(first: Boolean, last: Boolean)


object TarArchive {
  def subflowPerEntry(cancelStrategy: SubstreamCancelStrategy = SubstreamCancelStrategy.propagate) =
    flow()
      .splitWhen(cancelStrategy)({
        case (_, _, TarArchiveEntryRecordOrder(true, _)) => true
        case _ => false
      })
      .map(tuple => (tuple._1, tuple._2))

  def flow(): Flow[ByteString, (TarArchiveEntry, ByteString, TarArchiveEntryRecordOrder), NotUsed] =
    Flow[ByteString]
      .via(LevelByteStringsSize(512))
      .via(TarArchive())
}

case class TarArchive() extends GraphStage[FlowShape[ByteString, (TarArchiveEntry, ByteString, TarArchiveEntryRecordOrder)]] {
  val in: Inlet[ByteString] = Inlet("TarArchive.in")
  val out: Outlet[(TarArchiveEntry, ByteString, TarArchiveEntryRecordOrder)] = Outlet("TarArchive.out")
  override val shape: FlowShape[ByteString, (TarArchiveEntry, ByteString, TarArchiveEntryRecordOrder)] = FlowShape.of(in, out)

  // We rely on all input ByteStrings being 512 bytes long, this is accomplished by attaching a LevelByteStringsSize(512)
  // Flow to the input of this Flow. Also no support for tar extensions or long filenames. But this does work fine for openimages tar files.

  val firstRecord = TarArchiveEntryRecordOrder(first = true, last = false)
  val middleRecord = TarArchiveEntryRecordOrder(first = false, last = false)
  val lastRecord = TarArchiveEntryRecordOrder(first = false, last = true)
  val firstLastRecord = TarArchiveEntryRecordOrder(first = true, last = true)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    var curEntry: Option[TarArchiveEntry] = None
    var recordsRemaining: Long = 0
    var nextRecordOrder: TarArchiveEntryRecordOrder = firstRecord
    var readEOF: Boolean = false

    override def onPull(): Unit = {
      pull(in)
    }

    override def onPush(): Unit = {
      val chunk: ByteString = grab(in)

      if (chunk.length != 512) {
        failStage(InvalidTarRecordSize(chunk.length))
      }

      if (recordsRemaining > 1) {
        push(out, (curEntry.get, chunk, nextRecordOrder))
        recordsRemaining -= 1
        if (recordsRemaining == 1) {
          nextRecordOrder = lastRecord
        } else {
          nextRecordOrder = middleRecord
        }
      } else if (recordsRemaining == 1) {
        push(out, (curEntry.get, chunk.slice(0, (curEntry.get.getSize % 512).toInt), nextRecordOrder))
        recordsRemaining = 0
        nextRecordOrder = firstRecord
      } else {
        val recordIsEOF = chunk.forall(b => b == 0)
        if (recordIsEOF) {
          curEntry = None
          recordsRemaining = 0
          nextRecordOrder = firstRecord
          readEOF = true
          pull(in)
        } else {
          curEntry = Some(new TarArchiveEntry(chunk.toArray))
          if (!curEntry.get.isCheckSumOK) {
            failStage(TarFileCorruptedException())
          }
          recordsRemaining = curEntry.get.getSize / 512 + (if (curEntry.get.getSize % 512 != 0) 1 else 0)
          nextRecordOrder =
            if (recordsRemaining > 1) {
              firstRecord
            } else {
              firstLastRecord
            }
          if (recordsRemaining == 0) {
            push(out, (curEntry.get, ByteString(), nextRecordOrder))
          } else {
            pull(in)
          }
        }
      }
    }

    override def onUpstreamFinish(): Unit = {
      if (recordsRemaining > 0) {
        failStage(InvalidTarFile(recordsRemaining))
      }
      if (!readEOF) {
        failStage(NoEOFInUpstream())
      }
      completeStage()
    }

    setHandlers(in, out, this)
  }

  case class InvalidTarRecordSize(length: Int) extends RuntimeException(s"Received an invalid record size: $length. Must be 512")

  case class InvalidTarFile(recordsRemaining: Long) extends RuntimeException(s"Expected $recordsRemaining more records")

  case class NoEOFInUpstream() extends RuntimeException("Didn't read EOF record. Tar file is corrupt.")
}
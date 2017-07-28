package nuffer.oidl

import java.io.InputStream

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}

/**
  * The purpose of this class is to prevent close() from being called on a TarArchiveInputStream when streaming a
  * single entry in the tar file.
  */
case class TarEntryInputStream(is: TarArchiveInputStream) extends InputStream {
  override def read(): Int = is.read()
  override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
}

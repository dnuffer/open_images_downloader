package nuffer.oidl

import akka.Done
import akka.util.ByteString

import scala.util.Try

final case class DigestResult(messageDigest: ByteString, status: Try[Done])

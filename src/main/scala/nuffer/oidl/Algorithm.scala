package nuffer.oidl

sealed trait Algorithm

object Algorithm {

  case object MD2 extends Algorithm

  case object MD5 extends Algorithm

  case object `SHA-1` extends Algorithm

  case object `SHA-256` extends Algorithm

  case object `SHA-384` extends Algorithm

  case object `SHA-512` extends Algorithm

}

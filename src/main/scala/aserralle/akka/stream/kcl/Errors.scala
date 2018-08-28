/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

object Errors {
  case class WorkerUnexpectedShutdown(cause: Throwable) extends Throwable(cause)

  case class BackpressureTimeout(cause: Throwable) extends Throwable(cause)
}

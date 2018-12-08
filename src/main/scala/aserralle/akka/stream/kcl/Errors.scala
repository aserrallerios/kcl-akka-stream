/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

object Errors {
  sealed class KinesisWorkerSourceError(err: Throwable) extends Throwable(err)

  case class WorkerUnexpectedShutdown(cause: Throwable)
      extends KinesisWorkerSourceError(cause)

  case class BackpressureTimeout(cause: Throwable)
      extends KinesisWorkerSourceError(cause)
}

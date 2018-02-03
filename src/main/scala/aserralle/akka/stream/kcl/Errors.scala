/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

import scala.util.control.NoStackTrace

object Errors {

  sealed trait KinesisWorkerSourceError extends NoStackTrace
  case class WorkerUnexpectedShutdown(cause: Throwable)
      extends KinesisWorkerSourceError

}

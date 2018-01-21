/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

import com.amazonaws.services.kinesis.model.PutRecordsResultEntry

import scala.util.control.NoStackTrace

object KinesisErrors {

  sealed trait KinesisSourceError extends NoStackTrace
  case object NoShardsError extends KinesisSourceError
  case object GetShardIteratorError extends KinesisSourceError
  case object GetRecordsError extends KinesisSourceError

  sealed trait KinesisFlowErrors extends NoStackTrace
  case class FailurePublishingRecords(e: Exception)
      extends RuntimeException(e)
      with KinesisFlowErrors
  case class ErrorPublishingRecords(attempts: Int,
                                    records: Seq[PutRecordsResultEntry])
      extends RuntimeException(
        s"Unable to publish records after $attempts attempts")
      with KinesisFlowErrors

  sealed trait KinesisWorkerSourceError extends NoStackTrace
  case object WorkerUnexpectedShutdown extends KinesisWorkerSourceError

}

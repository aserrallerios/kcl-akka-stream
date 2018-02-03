/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

import akka.Done
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber
import com.amazonaws.services.kinesis.model.Record

import scala.concurrent.{ExecutionContext, Future}

class CommittableRecord(
    val shardId: String,
    val recordProcessorStartingSequenceNumber: ExtendedSequenceNumber,
    val millisBehindLatest: Long,
    val record: Record,
    recordProcessor: IRecordProcessor,
    checkpointer: IRecordProcessorCheckpointer
)(implicit executor: ExecutionContext) {

  val sequenceNumber: String = record.getSequenceNumber

  def recordProcessorShutdownReason(): Option[ShutdownReason] =
    recordProcessor.shutdown
  def canBeCheckpointed(): Boolean =
    recordProcessorShutdownReason().isEmpty
  def tryToCheckpoint(): Future[Done] =
    Future {
      checkpointer.checkpoint(record)
      Done
    }

}

object CommittableRecord {

  // Only makes sense to compare Records belonging to the same shard
  implicit val orderBySequenceNumber: Ordering[CommittableRecord] =
    Ordering.by(_.sequenceNumber)

}

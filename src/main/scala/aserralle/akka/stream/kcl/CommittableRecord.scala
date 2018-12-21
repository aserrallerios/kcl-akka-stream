/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

import akka.Done
import software.amazon.kinesis.lifecycle.ShutdownReason
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.concurrent.{ExecutionContext, Future}

class CommittableRecord(
    val shardId: String,
    val recordProcessorStartingSequenceNumber: ExtendedSequenceNumber,
    val millisBehindLatest: Long,
    val record: KinesisClientRecord,
    recordProcessor: ShardProcessor,
    checkpointer: RecordProcessorCheckpointer
)(implicit executor: ExecutionContext) {

  val sequenceNumber: String = record.sequenceNumber()
  val subSequenceNumber: Long = record.subSequenceNumber()

  def recordProcessorShutdownReason(): Option[ShutdownReason] =
    recordProcessor.shutdown

  def canBeCheckpointed(): Boolean =
    recordProcessorShutdownReason().isEmpty

  def tryToCheckpoint(): Future[Done] =
    Future {
      checkpointer.checkpoint()
      Done
    }
}

object CommittableRecord {

  // Only makes sense to compare Records belonging to the same shard
  // Records that have been batched by the KPL producer all have the
  // same sequence number but will differ by subsequence number
  implicit val orderBySequenceNumber: Ordering[CommittableRecord] =
    Ordering[(String, Long)].on(cr ⇒ (cr.sequenceNumber, cr.subSequenceNumber))
}

/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

import akka.Done
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.{
  ExtendedSequenceNumber,
  UserRecord
}
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
  // Records that have been batched by the KCL producer all have the
  // same sequence number but will differ by subsequence number
  implicit val orderBySequenceNumber: Ordering[CommittableRecord] =
    Ordering[(String, Long)].on(cr ⇒
      (cr.sequenceNumber, cr.record match {
        case ur: UserRecord ⇒ ur.getSubSequenceNumber
        case _ ⇒ 0
      }))

}

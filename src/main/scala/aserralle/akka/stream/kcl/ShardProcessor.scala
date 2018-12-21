/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

import software.amazon.kinesis.lifecycle.ShutdownReason
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.ShardRecordProcessor
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

private[kcl] class ShardProcessor(
    callback: CommittableRecord => Unit,
    terminateStreamGracePeriod: FiniteDuration
)(implicit executionContext: ExecutionContext)
    extends ShardRecordProcessor {

  private var shardId: String = _
  private var extendedSequenceNumber: ExtendedSequenceNumber = _

  var shutdown: Option[ShutdownReason] = None

  override def initialize(initializationInput: InitializationInput): Unit = {
    shardId = initializationInput.shardId()
    extendedSequenceNumber = initializationInput.extendedSequenceNumber()
  }

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    processRecordsInput.records().asScala.foreach { record =>
      callback(
        new CommittableRecord(
          shardId,
          extendedSequenceNumber,
          processRecordsInput.millisBehindLatest(),
          record,
          recordProcessor = this,
          processRecordsInput.checkpointer
        )
      )
    }
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {}

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
    // We need to checkpoint, but if we do it immediately any records still
    // in flight may get lost, so we wait for the grace period
    shutdown = Some(ShutdownReason.SHARD_END)
    Thread.sleep(terminateStreamGracePeriod.toMillis)
    shardEndedInput.checkpointer.checkpoint()
  }

  override def shutdownRequested(shutdownInput: ShutdownRequestedInput): Unit = {
    // We don't checkpoint at this point as we assume the
    // standard mechanism will checkpoint when required
    shutdown = Some(ShutdownReason.REQUESTED)
    Thread.sleep(terminateStreamGracePeriod.toMillis)
  }

}

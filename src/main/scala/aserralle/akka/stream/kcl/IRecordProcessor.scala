/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

import software.amazon.kinesis.lifecycle.{ShutdownInput, ShutdownReason}
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.{RecordProcessorCheckpointer, ShardRecordProcessor}
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

private[kcl] class IRecordProcessor(
    callback: CommittableRecord => Unit,
    terminateStreamGracePeriod: FiniteDuration
)(implicit executionContext: ExecutionContext)
    extends ShardRecordProcessor {

  private var shardId: String = _
  private var extendedSequenceNumber: ExtendedSequenceNumber = _

  var shutdown: Option[ShutdownReason] = None
  var latestCheckpointer: Option[RecordProcessorCheckpointer] = None

  override def initialize(initializationInput: InitializationInput): Unit = {
    shardId = initializationInput.shardId()
    extendedSequenceNumber = initializationInput.extendedSequenceNumber()
  }

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    val checkpointer = processRecordsInput.checkpointer
    latestCheckpointer = Some(checkpointer)
    processRecordsInput.records().asScala.foreach { record =>
      callback(
        new CommittableRecord(
          shardId,
          extendedSequenceNumber,
          processRecordsInput.millisBehindLatest(),
          record,
          recordProcessor = this,
          checkpointer
        )
      )
    }
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {}

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {}

  override def shutdownRequested(shutdownInput: ShutdownRequestedInput): Unit = {

    // we need to checkpoint, but if we do it immediately any records still
    // in flight may get lost
    latestCheckpointer = Some(shutdownInput.checkpointer)
  }

//  override def shutdown(shutdownInput: ShutdownInput): Unit = {
//    shutdown = Some(shutdownInput.shutdownReason)
//    shutdownInput.shutdownReason match {
//      case ShutdownReason.TERMINATE =>
//        Thread.sleep(terminateStreamGracePeriod.toMillis)
//      case ShutdownReason.ZOMBIE => ()
//      case ShutdownReason.REQUESTED => ()
//    }
//  }

}

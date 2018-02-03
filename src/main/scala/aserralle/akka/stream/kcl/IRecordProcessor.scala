/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.{
  ExtendedSequenceNumber,
  InitializationInput,
  ProcessRecordsInput,
  ShutdownInput
}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

private[kcl] class IRecordProcessor(
    callback: CommittableRecord => Unit,
    terminateStreamGracePeriod: FiniteDuration
)(implicit executionContext: ExecutionContext)
    extends com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor {
  private var shardId: String = _
  private var extendedSequenceNumber: ExtendedSequenceNumber = _
  var shutdown: Option[ShutdownReason] = None
  var latestCheckpointer: Option[IRecordProcessorCheckpointer] = None

  override def initialize(initializationInput: InitializationInput): Unit = {
    shardId = initializationInput.getShardId
    extendedSequenceNumber = initializationInput.getExtendedSequenceNumber
  }

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    latestCheckpointer = Some(processRecordsInput.getCheckpointer)
    processRecordsInput.getRecords.asScala.foreach { record =>
      callback(
        new CommittableRecord(
          shardId,
          extendedSequenceNumber,
          processRecordsInput.getMillisBehindLatest,
          record,
          recordProcessor = this,
          processRecordsInput.getCheckpointer
        )
      )
    }
  }

  override def shutdown(shutdownInput: ShutdownInput): Unit = {
    shutdown = Some(shutdownInput.getShutdownReason)
    latestCheckpointer = Some(shutdownInput.getCheckpointer)
    shutdownInput.getShutdownReason match {
      case ShutdownReason.TERMINATE =>
        Thread.sleep(terminateStreamGracePeriod.toMillis)
      case ShutdownReason.ZOMBIE => ()
      case ShutdownReason.REQUESTED => ()
    }
  }

}

/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

import java.nio.ByteBuffer
import java.time.Instant
import java.util.concurrent.Semaphore

import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import aserralle.akka.stream.kcl.Errors.WorkerUnexpectedShutdown
import aserralle.akka.stream.kcl.scaladsl.KinesisWorkerSource
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpecLike}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.lifecycle.events.{InitializationInput, ProcessRecordsInput, ShutdownRequestedInput}
import software.amazon.kinesis.processor.{RecordProcessorCheckpointer, ShardRecordProcessor, ShardRecordProcessorFactory}
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Failure

class KinesisWorkerSourceSourceSpec
    extends WordSpecLike
    with Matchers
    with DefaultTestContext
    with Eventually {

  "KinesisWorker Source" must {

    "publish records downstream" in new KinesisWorkerContext with TestData {
      val initializationInput: InitializationInput =
        initializationInput("shard")
      recordProcessor.initialize(initializationInput)
      recordProcessor.processRecords(recordsInput)

      val producedRecord = sinkProbe.requestNext()
      producedRecord.recordProcessorStartingSequenceNumber shouldBe initializationInput.extendedSequenceNumber()
      producedRecord.shardId shouldBe initializationInput.shardId()
//      producedRecord.millisBehindLatest shouldBe recordsInput.getMillisBehindLatest
      producedRecord.record shouldBe record

      killSwitch.shutdown()

      sinkProbe.expectComplete()
    }

    "publish records downstream using different IRecordProcessor incarnations" in new KinesisWorkerContext
    with TestData {
      val initializationInput: InitializationInput =
        initializationInput("shard")

      recordProcessor.initialize(initializationInput)
      recordProcessor.processRecords(recordsInput)

      var producedRecord = sinkProbe.requestNext()
      producedRecord.recordProcessorStartingSequenceNumber shouldBe initializationInput.extendedSequenceNumber()
      producedRecord.shardId shouldBe initializationInput.shardId()
//      producedRecord.millisBehindLatest shouldBe recordsInput.getMillisBehindLatest
      producedRecord.record shouldBe record

      val newRecordProcessor = recordProcessorFactory.shardRecordProcessor()

      newRecordProcessor.initialize(initializationInput)
      newRecordProcessor.processRecords(recordsInput)

      producedRecord = sinkProbe.requestNext()
      producedRecord.recordProcessorStartingSequenceNumber shouldBe initializationInput.extendedSequenceNumber()
      producedRecord.shardId shouldBe initializationInput.shardId()
//      producedRecord.millisBehindLatest shouldBe recordsInput.getMillisBehindLatest
      producedRecord.record shouldBe record

      killSwitch.shutdown()

      sinkProbe.expectComplete()
    }

    "call Worker shutdown on stage completion" in new KinesisWorkerContext {
      killSwitch.shutdown()

      sinkProbe.expectComplete()
      eventually {
        verify(worker).run()
        verify(worker).shutdown()
      }
    }

    "complete the stage if the Worker is shutdown" in new KinesisWorkerContext {
      lock.release()
      sinkProbe.expectComplete()
      eventually {
        verify(worker).run()
      }
    }

    "complete the stage with error if the Worker fails" in new KinesisWorkerContext(
      Some(WorkerUnexpectedShutdown(new RuntimeException()))) {
      sinkProbe.expectError() shouldBe a[WorkerUnexpectedShutdown]
      eventually {
        verify(worker).run()
      }
    }

    "not drop messages in case of back-pressure" in new KinesisWorkerContext
    with TestData {
      recordProcessor.initialize(initializationInput("shard"))
      for (i <- 1 to 10) { // 10 is a buffer size
        val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
        when(record.sequenceNumber).thenReturn(i.toString)
        recordProcessor.processRecords(
          recordsInput.toBuilder.records(List(record).asJava).build())
      }
      //expect to consume all 10
      for (_ <- 1 to 10) sinkProbe.requestNext()

      //send another batch to exceed the queue size
      Future {
        for (i <- 1 to 25) { // 10 is a buffer size
          val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
          when(record.sequenceNumber).thenReturn(i.toString)
          recordProcessor.processRecords(
            recordsInput.toBuilder.records(List(record).asJava).build())
        }
      }

      //expect to consume all 25 with slow consumer
      for (_ <- 1 to 25) {
        sinkProbe.requestNext()
        Thread.sleep(100)
      }

      killSwitch.shutdown()
      sinkProbe.expectComplete()
    }

    "not drop messages in case of back-pressure with multiple shard workers" in new KinesisWorkerContext
    with TestData {
      recordProcessor.initialize(initializationInput("shard1"))
      recordProcessor2.initialize(initializationInput("shard2"))

      for (i <- 1 to 5) { // 10 is a buffer size
        val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
        when(record.sequenceNumber).thenReturn(i.toString)
        recordProcessor.processRecords(
          recordsInput.toBuilder.records(List(record).asJava).build())
        recordProcessor2.processRecords(
          recordsInput.toBuilder.records(List(record).asJava).build())
      }

      //expect to consume all 10 across both shards
      for (_ <- 1 to 10) sinkProbe.requestNext()

      // Each shard is assigned its own worker thread, so we get messages
      // from each thread simultaneously.
      def simulateWorkerThread(rp: ShardRecordProcessor): Future[Unit] = {
        Future {
          for (i <- 1 to 25) { // 10 is a buffer size
            val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
            when(record.sequenceNumber).thenReturn(i.toString)
            rp.processRecords(recordsInput.toBuilder.records(List(record).asJava).build())
          }
        }
      }

      //send another batch to exceed the queue size - this is shard 1
      simulateWorkerThread(recordProcessor)

      //send another batch to exceed the queue size - this is shard 2
      simulateWorkerThread(recordProcessor2)

      //expect to consume all 50 with slow consumer
      for (_ <- 1 to 50) {
        sinkProbe.requestNext()
        Thread.sleep(100)
      }

      killSwitch.shutdown()
      sinkProbe.expectComplete()
    }

    "stop the stream when back pressure timeout elapsed" in new KinesisWorkerContext(
      backpressureTimeout = 100.milliseconds) with TestData {
      recordProcessor.initialize(initializationInput("shard"))
      //Fast consumer sends 25 messages into 10 items queue size
      for (i <- 1 to 25) { // 10 is a buffer size
        val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
        when(record.sequenceNumber).thenReturn(i.toString)
        recordProcessor.processRecords(
          recordsInput.toBuilder.records(List(record).asJava).build())
      }

      Await.ready(watch, 5.seconds)
      val Failure(exception) = watch.value.get
      assert(
        exception.getCause.getMessage
          .contains("Futures timed out after [100 milliseconds]"))

      killSwitch.shutdown()
    }
  }

  private abstract class KinesisWorkerContext(
      workerFailure: Option[Throwable] = None,
      backpressureTimeout: FiniteDuration = 1.minute) {

    protected val worker = org.mockito.Mockito.mock(classOf[Scheduler])

    val lock = new Semaphore(0)
    when(worker.run()).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit =
        workerFailure.fold(lock.acquire())(throw _)
    })

    val semaphore = new Semaphore(0)

    var recordProcessorFactory: ShardRecordProcessorFactory = _
    var recordProcessor: ShardRecordProcessor = _
    var recordProcessor2: ShardRecordProcessor = _
    val workerBuilder = { x: ShardRecordProcessorFactory =>
      recordProcessorFactory = x
      recordProcessor = x.shardRecordProcessor()
      recordProcessor2 = x.shardRecordProcessor()
      semaphore.release()
      worker
    }
    val ((killSwitch, watch), sinkProbe) =
      KinesisWorkerSource(
        workerBuilder,
        KinesisWorkerSourceSettings(bufferSize = 10,
                                    terminateStreamGracePeriod = 1.second,
                                    backpressureTimeout = backpressureTimeout))
        .viaMat(KillSwitches.single)(Keep.right)
        .watchTermination()(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

    watch.onComplete(_ => lock.release())

    sinkProbe.ensureSubscription()
    sinkProbe.request(1)

    semaphore.acquire()
  }

  private trait TestData {
    protected val checkpointer =
      org.mockito.Mockito.mock(classOf[RecordProcessorCheckpointer])

    def initializationInput(shardId: String) =
      InitializationInput.builder()
        .shardId(shardId)
        .extendedSequenceNumber(ExtendedSequenceNumber.AT_TIMESTAMP)
        .build()

    val record =
      KinesisClientRecord.fromRecord(
        Record.builder()
          .approximateArrivalTimestamp(Instant.now())
          .encryptionType("encryption")
          .partitionKey("partitionKey")
          .sequenceNumber("sequenceNum")
          .data(SdkBytes.fromByteBuffer(ByteBuffer.wrap(Array[Byte](1))))
          .build())

    val recordsInput =
      ProcessRecordsInput.builder()
        .checkpointer(checkpointer)
        .millisBehindLatest(1L)
        .records(List(record).asJava)
        .build()
  }

  "KinesisWorker checkpoint Flow " must {

    "checkpoint batch of records with same sequence number" in new KinesisWorkerCheckpointContext {
      val recordProcessor = new ShardProcessor(_ => (), 1.second)

      val checkpointerShard1 =
        org.mockito.Mockito.mock(classOf[RecordProcessorCheckpointer])
      var latestRecordShard1: KinesisClientRecord = _
      for (i <- 1 to 3) {
        val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
        when(record.sequenceNumber).thenReturn("1")
        when(record.subSequenceNumber).thenReturn(i.toLong)
        sourceProbe.sendNext(
          new CommittableRecord(
            "shard-1",
            org.mockito.Mockito.mock(classOf[ExtendedSequenceNumber]),
            1L,
            record,
            recordProcessor,
            checkpointerShard1
          )
        )
        latestRecordShard1 = record
      }

      for (_ <- 1 to 3) sinkProbe.requestNext()

      eventually(verify(checkpointerShard1)
        .checkpoint())

      sourceProbe.sendComplete()
      sinkProbe.expectComplete()
    }

    "checkpoint batch of records of different shards" in new KinesisWorkerCheckpointContext {
      val recordProcessor = new ShardProcessor(_ => (), 1.second)

      val checkpointerShard1 =
        org.mockito.Mockito.mock(classOf[RecordProcessorCheckpointer])
      var latestRecordShard1: KinesisClientRecord = _
      for (i <- 1 to 3) {
        val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
        when(record.sequenceNumber).thenReturn(i.toString)
        sourceProbe.sendNext(
          new CommittableRecord(
            "shard-1",
            org.mockito.Mockito.mock(classOf[ExtendedSequenceNumber]),
            1L,
            record,
            recordProcessor,
            checkpointerShard1
          )
        )
        latestRecordShard1 = record
      }
      val checkpointerShard2 =
        org.mockito.Mockito.mock(classOf[RecordProcessorCheckpointer])
      var latestRecordShard2: KinesisClientRecord = _
      for (i <- 1 to 3) {
        val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
        when(record.sequenceNumber).thenReturn(i.toString)
        sourceProbe.sendNext(
          new CommittableRecord(
            "shard-2",
            org.mockito.Mockito.mock(classOf[ExtendedSequenceNumber]),
            1L,
            record,
            recordProcessor,
            checkpointerShard2
          )
        )
        latestRecordShard2 = record
      }

      for (_ <- 1 to 6) sinkProbe.requestNext()

      eventually {
        verify(checkpointerShard1)
          .checkpoint()
        verify(checkpointerShard2)
          .checkpoint()
      }

      sourceProbe.sendComplete()
      sinkProbe.expectComplete()
    }

    "not checkpoint the batch if the IRecordProcessor has been shutdown" in new KinesisWorkerCheckpointContext {
      val recordProcessor = new ShardProcessor(_ => (), 1.second)
      recordProcessor.shutdownRequested(
        ShutdownRequestedInput.builder().checkpointer(org.mockito.Mockito.mock(classOf[RecordProcessorCheckpointer])).build())

      val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
      when(record.sequenceNumber).thenReturn("1")
      val committableRecord = new CommittableRecord(
        "shard-1",
        org.mockito.Mockito.mock(classOf[ExtendedSequenceNumber]),
        1L,
        record,
        recordProcessor,
        org.mockito.Mockito.mock(classOf[RecordProcessorCheckpointer])
      )
      sourceProbe.sendNext(committableRecord)

      sinkProbe.requestNext()

      sourceProbe.sendComplete()
      sinkProbe.expectComplete()
    }

    "fail with Exception if checkpoint action fails" in new KinesisWorkerCheckpointContext {
      val recordProcessor = new ShardProcessor(_ => (), 1.second)
      val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
      when(record.sequenceNumber).thenReturn("1")
      val checkpointer =
        org.mockito.Mockito.mock(classOf[RecordProcessorCheckpointer])
      val committableRecord = new CommittableRecord(
        "shard-1",
        org.mockito.Mockito.mock(classOf[ExtendedSequenceNumber]),
        1L,
        record,
        recordProcessor,
        checkpointer
      )
      sourceProbe.sendNext(committableRecord)

      val failure = new RuntimeException()
      when(checkpointer.checkpoint()).thenThrow(failure)

      sinkProbe.request(1)

      sinkProbe.expectError(failure)
    }

  }

  private trait KinesisWorkerCheckpointContext {
    val (sourceProbe, sinkProbe) =
      TestSource
        .probe[CommittableRecord]
        .via(
          KinesisWorkerSource
            .checkpointRecordsFlow(
              KinesisWorkerCheckpointSettings(maxBatchSize = 100,
                                              maxBatchWait = 500.millis))
        )
        .toMat(TestSink.probe)(Keep.both)
        .run()
  }

}

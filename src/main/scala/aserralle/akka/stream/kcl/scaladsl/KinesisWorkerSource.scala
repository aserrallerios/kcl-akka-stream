/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl.scaladsl

import java.util.concurrent.Semaphore

import akka.stream.Supervision.{Resume, Stop}
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, Zip}
import akka.{Done, NotUsed}
import aserralle.akka.stream.kcl.Errors.{BackpressureTimeout, WorkerUnexpectedShutdown}
import aserralle.akka.stream.kcl.{
  CommittableRecord,
  KinesisWorkerCheckpointSettings,
  KinesisWorkerSourceSettings,
  ShardProcessor}
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.exceptions.ShutdownException
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.Exception
import scala.util.{Failure, Success}

object KinesisWorkerSource {

  def apply(
    workerBuilder: ShardRecordProcessorFactory => Scheduler,
    settings: KinesisWorkerSourceSettings =
      KinesisWorkerSourceSettings.defaultInstance
  )(implicit workerExecutor: ExecutionContext)
    : Source[CommittableRecord, Scheduler] =
    Source
      .queue[CommittableRecord](settings.bufferSize,
      OverflowStrategy.backpressure)
      .watchTermination()(Keep.both)
      .mapMaterializedValue {
        case (queue, watch) =>
          val semaphore = new Semaphore(1, true)
          val worker = workerBuilder(
            () => {
              new ShardProcessor(
                record => {
                  semaphore.acquire(1)
                  (Exception.nonFatalCatch either Await.result(
                    queue.offer(record),
                    settings.backpressureTimeout) left)
                    .foreach(err => queue.fail(BackpressureTimeout(err)))
                  semaphore.release()
                },
                settings.terminateStreamGracePeriod)
            }
          )

          Future(worker.run()).onComplete {
            case Failure(ex) =>
              queue.fail(WorkerUnexpectedShutdown(ex))
            case Success(_) => queue.complete()
          }
          watch.onComplete(_ => Future(worker.shutdown()))
          worker
      }

  def checkpointRecordsFlow(
    settings: KinesisWorkerCheckpointSettings =
      KinesisWorkerCheckpointSettings.defaultInstance
  ): Flow[CommittableRecord, KinesisClientRecord, NotUsed] =
    Flow[CommittableRecord]
      .groupBy(MAX_KINESIS_SHARDS, _.shardId)
      .groupedWithin(settings.maxBatchSize, settings.maxBatchWait)
      .via(GraphDSL.create() { implicit b ⇒
        import GraphDSL.Implicits._

        val `{` =
          b.add(scaladsl.Broadcast[immutable.Seq[CommittableRecord]](2))
        val `}` = b.add(Zip[Done, immutable.Seq[CommittableRecord]])
        val `=` = b.add(Flow[KinesisClientRecord])

        `{`.out(0)
          .map(_.max)
          .mapAsync(1)(r =>
            if (r.canBeCheckpointed()) r.tryToCheckpoint()
            else Future.successful(Done)) ~> `}`.in0
        `{`.out(1) ~> `}`.in1

        `}`.out.map(_._2).mapConcat(identity).map(_.record) ~> `=`

        FlowShape(`{`.in, `=`.out)
      })
      .mergeSubstreams
      .withAttributes(ActorAttributes.supervisionStrategy {
        case _: ShutdownException =>
          Resume
        case _ => Stop
      })

  def checkpointRecordsSink(
    settings: KinesisWorkerCheckpointSettings =
      KinesisWorkerCheckpointSettings.defaultInstance
  ): Sink[CommittableRecord, NotUsed] =
    checkpointRecordsFlow(settings).to(Sink.ignore)

  // http://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
  private val MAX_KINESIS_SHARDS = 500

}

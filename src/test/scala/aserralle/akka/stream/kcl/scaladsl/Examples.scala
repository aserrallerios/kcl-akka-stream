/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl.scaladsl

import java.util.UUID
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import aserralle.akka.stream.kcl.{KinesisWorkerCheckpointSettings, KinesisWorkerSourceSettings}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.polling.{SimpleRecordsFetcherFactory, SynchronousBlockingRetrievalFactory}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object Examples {

  //#init-system
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  //#init-system

  //#init-clients
  val region: Region = Region.EU_WEST_1
  val kinesisClient: KinesisAsyncClient = KinesisAsyncClient.builder.region(region).build
  val dynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient.builder.region(region).build
  val cloudWatchClient: CloudWatchAsyncClient = CloudWatchAsyncClient.builder.region(region).build
  //#init-clients

  //#worker-settings
  val workerSourceSettings = KinesisWorkerSourceSettings(
    bufferSize = 1000,
    terminateStreamGracePeriod = 1 minute,
    backpressureTimeout = 1 minute)

  val builder: ShardRecordProcessorFactory => Scheduler = recordProcessorFactory => {

    val streamName = "myStreamName"

    val configsBuilder = new ConfigsBuilder(
      streamName,
      "myApp",
      kinesisClient,
      dynamoClient,
      cloudWatchClient,
      s"${
        import scala.sys.process._
        "hostname".!!.trim()
      }:${UUID.randomUUID()}",
      recordProcessorFactory)

    //#Fan-out retrievalConfig - will incur additional AWS costs
    val fanoutRetrievalConfig = configsBuilder.retrievalConfig
    //#Fan-out retrievalConfig

    //#Non-fan-out retrievalConfig i.e. equivalent of KCL 1 client
    val retrievalConfig =
      configsBuilder.retrievalConfig
          .retrievalFactory(
            new SynchronousBlockingRetrievalFactory(
              streamName,
              kinesisClient,
              new SimpleRecordsFetcherFactory,
              1000))
    //#Non-fan-out retrievalConfig


    new Scheduler(
      configsBuilder.checkpointConfig,
      configsBuilder.coordinatorConfig,
      configsBuilder.leaseManagementConfig,
      configsBuilder.lifecycleConfig,
      configsBuilder.metricsConfig,
      configsBuilder.processorConfig,
      configsBuilder.retrievalConfig)
  }
  //#worker-settings

  //#worker-source
  implicit val executor =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1000))

  KinesisWorkerSource(builder, workerSourceSettings).to(Sink.ignore)
  //#worker-source

  //#checkpoint
  val checkpointSettings = KinesisWorkerCheckpointSettings(100, 30 seconds)
  KinesisWorkerSource(builder, workerSourceSettings)
    .via(KinesisWorkerSource.checkpointRecordsFlow(checkpointSettings))
    .to(Sink.ignore)
  KinesisWorkerSource(builder, workerSourceSettings).to(
    KinesisWorkerSource.checkpointRecordsSink(checkpointSettings))
  //#checkpoint

}

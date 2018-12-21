/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import aserralle.akka.stream.kcl.CommittableRecord;
import aserralle.akka.stream.kcl.KinesisWorkerCheckpointSettings;
import aserralle.akka.stream.kcl.KinesisWorkerSourceSettings;
import scala.concurrent.duration.FiniteDuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.SimpleRecordsFetcherFactory;
import software.amazon.kinesis.retrieval.polling.SynchronousBlockingRetrievalFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Examples {

    //#init-client
    Region region = Region.AWS_GLOBAL;
    KinesisAsyncClient kinesisClient = KinesisAsyncClient.builder().region(region).build();
    DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
    CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();
    //#init-client

    //#init-system
    final ActorSystem system = ActorSystem.create();
    final ActorMaterializer materializer = ActorMaterializer.create(system);
    //#init-system

    //#worker-settings
    final KinesisWorkerSource.WorkerBuilder workerBuilder = new KinesisWorkerSource.WorkerBuilder() {
        @Override
        public Scheduler build(ShardRecordProcessorFactory recordProcessorFactory) {

            String streamName = "myStreamName";

            ConfigsBuilder configsBuilder =
                    new ConfigsBuilder(
                            streamName,
                            "myApp",
                            kinesisClient,
                            dynamoClient,
                            cloudWatchClient,
                            "workerId",
                            recordProcessorFactory);

            //#Fan-out retrievalConfig - will incur additional AWS costs
            RetrievalConfig fanoutRetrievalConfig =
                    configsBuilder.retrievalConfig();
            //#Fan-out retrievalConfig

            //#Non-fan-out retrievalConfig i.e. equivalent of KCL 1 client
            RetrievalConfig retrievalConfig =
                    configsBuilder.retrievalConfig().retrievalFactory(
                            new SynchronousBlockingRetrievalFactory(
                                    streamName,
                                    kinesisClient,
                                    new SimpleRecordsFetcherFactory(),
                                    1000));
            //#Non-fan-out retrievalConfig

            return new Scheduler(
                    configsBuilder.checkpointConfig(),
                    configsBuilder.coordinatorConfig(),
                    configsBuilder.leaseManagementConfig(),
                    configsBuilder.lifecycleConfig(),
                    configsBuilder.metricsConfig(),
                    configsBuilder.processorConfig(),
                    retrievalConfig);
        }
    };

    final KinesisWorkerSourceSettings workerSettings = KinesisWorkerSourceSettings.create(
            1000,
            FiniteDuration.apply(1L, TimeUnit.SECONDS), FiniteDuration.apply(1L, TimeUnit.MINUTES));
    //#worker-settings

    //#worker-source
    final Executor workerExecutor = Executors.newFixedThreadPool(100);
    final Source<CommittableRecord, Scheduler> workerSource =
        KinesisWorkerSource.create(workerBuilder, workerSettings, workerExecutor );
    //#worker-source

    //#checkpoint
    final KinesisWorkerCheckpointSettings checkpointSettings =
            KinesisWorkerCheckpointSettings.create(1000, FiniteDuration.apply(30L, TimeUnit.SECONDS));
    final Flow<CommittableRecord, KinesisClientRecord, NotUsed> checkpointFlow =
        KinesisWorkerSource.checkpointRecordsFlow(checkpointSettings);
    //#checkpoint

}

/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import aserralle.akka.stream.kcl.KinesisWorkerCheckpointSettings;
import aserralle.akka.stream.kcl.KinesisWorkerSourceSettings;
import aserralle.akka.stream.kcl.CommittableRecord;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.Record;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Examples {

    //#init-client
    final AmazonKinesisAsync amazonKinesisAsync = AmazonKinesisAsyncClientBuilder.defaultClient();
    //#init-client

    //#init-system
    final ActorSystem system = ActorSystem.create();
    final ActorMaterializer materializer = ActorMaterializer.create(system);
    //#init-system

    //#worker-settings
    final KinesisWorkerSource.WorkerBuilder workerBuilder = new KinesisWorkerSource.WorkerBuilder() {
        @Override
        public Worker build(IRecordProcessorFactory recordProcessorFactory) {
            return new Worker.Builder()
                    .recordProcessorFactory(recordProcessorFactory)
                    .config(new KinesisClientLibConfiguration(
                            "myApp",
                            "myStreamName",
                            DefaultAWSCredentialsProviderChain.getInstance(),
                            "workerId"
                    ))
                    .build();
        }
    };
    final KinesisWorkerSourceSettings workerSettings = KinesisWorkerSourceSettings.create(
            1000,
            FiniteDuration.apply(1L, TimeUnit.SECONDS));
    //#worker-settings

    //#worker-source
    final Executor workerExecutor = Executors.newFixedThreadPool(100);
    final Source<CommittableRecord, Worker> workerSource = KinesisWorkerSource.create(workerBuilder, workerSettings, workerExecutor );
    //#worker-source

    //#checkpoint
    final KinesisWorkerCheckpointSettings checkpointSettings = KinesisWorkerCheckpointSettings.create(1000, FiniteDuration.apply(30L, TimeUnit.SECONDS));
    final Flow<CommittableRecord, Record, NotUsed> checkpointFlow = KinesisWorkerSource.checkpointRecordsFlow(checkpointSettings);
    //#checkpoint

}

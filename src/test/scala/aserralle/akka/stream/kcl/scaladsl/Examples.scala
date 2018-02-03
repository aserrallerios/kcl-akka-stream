/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl.scaladsl

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import aserralle.akka.stream.kcl.{
  KinesisWorkerCheckpointSettings,
  KinesisWorkerSourceSettings
}
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  KinesisClientLibConfiguration,
  Worker
}
import com.amazonaws.services.kinesis.{
  AmazonKinesisAsync,
  AmazonKinesisAsyncClientBuilder
}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object Examples {

  //#init-system
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  //#init-system

  //#init-client
  implicit val amazonKinesisAsync: AmazonKinesisAsync =
    AmazonKinesisAsyncClientBuilder.defaultClient()
  //#init-client

  //#worker-settings
  val workerSourceSettings = KinesisWorkerSourceSettings(
    bufferSize = 1000,
    terminateStreamGracePeriod = 1 minute)
  val builder: IRecordProcessorFactory => Worker = { recordProcessorFactory =>
    new Worker.Builder()
      .recordProcessorFactory(recordProcessorFactory)
      .config(
        new KinesisClientLibConfiguration(
          "myApp",
          "myStreamName",
          DefaultAWSCredentialsProviderChain.getInstance(),
          s"${
            import scala.sys.process._
            "hostname".!!.trim()
          }:${java.util.UUID.randomUUID()}"
        )
      )
      .build()
  }
  //#worker-settings

  //#worker-source
  implicit val _ =
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

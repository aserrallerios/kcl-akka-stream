/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Source
import aserralle.akka.stream.kcl.{scaladsl => _, _}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker

import scala.concurrent.ExecutionContext

object KinesisWorkerSourceV2 {
  def apply(workerBuilder: IRecordProcessorFactory => Worker,
            settings: KinesisWorkerSourceSettings =
              KinesisWorkerSourceSettings.defaultInstance)(
      implicit workerExecutor: ExecutionContext)
    : Source[CommittableRecord, NotUsed] =
    Source.fromGraph(new KinesisGraphStage(workerBuilder, settings))
}

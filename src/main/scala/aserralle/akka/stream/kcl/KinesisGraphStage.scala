/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

import java.util.concurrent.Semaphore

import akka.stream.stage.{
  GraphStage,
  GraphStageLogic,
  OutHandler,
  StageLogging
}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

final class KinesisGraphStage(workerBuilder: IRecordProcessorFactory => Worker,
                              settings: KinesisWorkerSourceSettings =
                                KinesisWorkerSourceSettings.defaultInstance)(
    implicit e: ExecutionContext)
    extends GraphStage[SourceShape[CommittableRecord]] {
  private val out = Outlet[CommittableRecord]("KinesisKclSource.out")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with StageLogging {
      private val bufferSize = settings.bufferSize
      private val queue = mutable.Queue[CommittableRecord]()
      private val backpressure = new Semaphore(bufferSize)

      private val handleError = getAsyncCallback[Throwable](e => {
        fail(out, e)
      })

      private def pushMessage(msg: CommittableRecord): Unit = {
        log.debug("pushing message downstream, releasing semaphore")
        push(out, msg)
        log.debug("releasing semaphore")
        backpressure.release()
      }

      private val handleMessage = getAsyncCallback[CommittableRecord](msg => {
        require(queue.size <= bufferSize)
        if (isAvailable(out)) {
          pushMessage(msg)
        } else {
          queue.enqueue(msg)
        }
      })

      override def preStart(): Unit = {
        val worker = workerBuilder(
          new IRecordProcessorFactory {
            override def createProcessor(): IRecordProcessor =
              new IRecordProcessor(record => {
                backpressure.acquire()
                handleMessage.invoke(record)
              }, settings.terminateStreamGracePeriod)
          }
        )
        Future(worker.run())
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          if (queue.nonEmpty) {
            pushMessage(queue.dequeue())
          }
      })
    }
  }

  override def shape: SourceShape[CommittableRecord] =
    SourceShape[CommittableRecord](out)
}

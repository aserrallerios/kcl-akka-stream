/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl

import java.util.concurrent.{Executors, TimeoutException}

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, blocking}

trait DefaultTestContext
    extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockitoSugar { this: Suite =>

  implicit protected val system: ActorSystem = ActorSystem()
  implicit protected val materializer: Materializer = ActorMaterializer()
  private val threadPool = Executors.newFixedThreadPool(10)
  implicit protected val executionContext =
    ExecutionContext.fromExecutor(threadPool)

  override protected def afterAll(): Unit = {
    Await.ready(system.terminate(), 5.seconds)
    threadPool.shutdown()
    if (!blocking(threadPool.awaitTermination(5, SECONDS)))
      throw new TimeoutException()
  }

}

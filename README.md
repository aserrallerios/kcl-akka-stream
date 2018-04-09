# Akka Stream Source for Kinesis Client Library [![travis-badge][]][travis] [![Bintray](https://img.shields.io/bintray/v/aserrallerios/maven/kcl-akka-stream.svg)]()

[travis]:                https://travis-ci.org/aserrallerios/kcl-akka-stream
[travis-badge]:          https://travis-ci.org/aserrallerios/kcl-akka-stream.svg?branch=master

For more information about Kinesis please visit the [official documentation](https://aws.amazon.com/documentation/kinesis/).

The KCL Source can read from several shards and rebalance automatically when other Workers are started or stopped. It also handles record sequence checkpoints.

For more information about KCL please visit the [official documentation](http://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html).

## Installation

```xml
<repository>
  <snapshots>
    <enabled>false</enabled>
  </snapshots>
  <id>bintray-<username>-maven</id>
  <name>bintray</name>
  <url>https://dl.bintray.com/content/aserrallerios/maven</url>
</repository>
...
<dependency>
  <groupId>aserrallerios</groupId>
  <artifactId>kcl-akka-stream_2.11</artifactId>
  <version>0.3</version>
  <type>pom</type>
</dependency>
```

```scala
resolvers += "aserrallerios bintray" at "https://dl.bintray.com/content/aserrallerios/maven"
libraryDependencies += "aserrallerios" %% "kcl-akka-stream" % "0.3"
```

## Usage

### AWS KCL Worker Source & checkpointer

The KCL Worker Source needs to create and manage Worker instances in order to consume records from Kinesis Streams.

In order to use it, you need to provide a Worker builder and the Source settings:

```scala
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
```

The Source also needs an `ExecutionContext` to run the Worker's thread and to commit/checkpoint records. Then the Source can be created as usual:

```scala
implicit val _ =
  ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1000))
KinesisWorkerSource(builder, workerSourceSettings).to(Sink.ignore)
```

### Committing records

The KCL Worker Source publishes messages downstream that can be committed in order to mark progression of consumers by shard. This process can be done manually or using the provided checkpointer Flow/Sink.

In order to use the Flow/Sink you must provide additional checkpoint settings:

```scala
val checkpointSettings = KinesisWorkerCheckpointSettings(100, 30 seconds)

KinesisWorkerSource(builder, workerSourceSettings)
  .via(KinesisWorker.checkpointRecordsFlow(checkpointSettings))
  .to(Sink.ignore)

KinesisWorkerSource(builder, workerSourceSettings).to(
  KinesisWorker.checkpointRecordsSink(checkpointSettings))
```

## License

Copyright (c) 2018 Albert Serrallé

This version of *kcl-akka-stream* is released under the Apache License, Version 2.0 (see LICENSE.txt).
By downloading and using this software you agree to the
[End-User License Agreement (EULA)](LICENSE).

We build on a number of third-party software tools, with the following licenses:

#### Java Libraries

Third-Party software        |   License
----------------------------|-----------------------
amazon-kinesis-client       | Amazon Software License
import sbt._, Keys._

object Dependencies {

  val AkkaVersion = sys.env.get("AKKA_SERIES") match {
    case Some("2.5") => "2.5.18"
    case _ => "2.4.20"
  }

  val AwsSdkVersion = "2.1.4"

  val Common = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion % Provided,
      "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
      "org.scalatest" %% "scalatest" % "3.0.1" % Test, // ApacheV2
      "com.novocode" % "junit-interface" % "0.11" % Test, // BSD-style
      "junit" % "junit" % "4.12" % Test // Eclipse Public License 1.0
    )
  )

  val Kinesis = Seq(
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "kinesis" % AwsSdkVersion % Provided, // Amazon Software License
      "software.amazon.kinesis" % "amazon-kinesis-client" % "2.0.5" % Provided, // Amazon Software License
      "org.mockito" % "mockito-core" % "2.7.11" % Test // MIT
    )
  )
}

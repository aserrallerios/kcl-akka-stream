import sbt._, Keys._

object Dependencies {

  val AkkaVersion = sys.env.get("AKKA_SERIES") match {
    case Some("2.5") => "2.5.6"
    case _ => "2.4.19"
  }

  val AwsSdkVersion = "1.11.226"

  val Common = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
      "org.scalatest" %% "scalatest" % "3.0.1" % Test, // ApacheV2
      "com.novocode" % "junit-interface" % "0.11" % Test, // BSD-style
      "junit" % "junit" % "4.12" % Test // Eclipse Public License 1.0
    )
  )

  val Kinesis = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-kinesis" % AwsSdkVersion, // ApacheV2
      "com.amazonaws" % "amazon-kinesis-client" % "1.8.8", // Amazon Software License
      "org.mockito"   % "mockito-core"     % "2.7.11"    % Test // MIT
    )
  )
}

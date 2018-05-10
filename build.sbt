lazy val `kcl-akka-stream` =
  Project(id = "kcl-akka-stream", base = file("."))
    .enablePlugins(AutomateHeaderPlugin)
    .settings(
      name := "kcl-akka-stream",
      // By default scalatest futures time out in 150 ms, dilate that to 600ms.
      // This should not impact the total test time as we don't expect to hit this
      // timeout.
      testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest,
                                            "-F",
                                            "4")
    )
    .settings(
      Dependencies.Kinesis,
      // For mockito
      parallelExecution in Test := false,
      onLoadMessage :=
        """
          |** Welcome to the sbt build definition for kcl-akka-stream! **
        """.stripMargin
    )

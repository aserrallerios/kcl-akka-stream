import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin
import com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin.autoImport._
import de.heikoseeberger.sbtheader._
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import sbtunidoc.BaseUnidocPlugin.autoImport._

object Common extends AutoPlugin {

  override def trigger = allRequirements

  override def requires = JvmPlugin && HeaderPlugin

  override lazy val projectSettings =
  Dependencies.Common ++ Seq(
    organization := "aserrallerios",
    homepage := Some(url("https://github.com/aserrallerios/kcl-akka-stream")),
    scmInfo := Some(ScmInfo(url("https://github.com/aserrallerios/kcl-akka-stream"), "git@github.com:aserrallerios/kcl-akka-stream.git")),
    developers += Developer("aserralle",
                            "Albert Serrallé",
                            "aserrallerios@gmail.com",
                            url("https://github.com/aserrallerios")),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    crossVersion := CrossVersion.binary,
    scalacOptions ++= Seq(
      "-encoding",
      "UTF-8",
      "-feature",
      "-unchecked",
      "-deprecation",
      //"-Xfatal-warnings",
      "-Xlint",
      "-Yno-adapted-args",
      "-Ywarn-dead-code",
      "-Xfuture"
    ),
    javacOptions in compile ++= Seq(
      "-Xlint:unchecked"
    ),
    // autoAPIMappings := true,
    // apiURL := Some(url()),
    // show full stack traces and test case durations
    testOptions in Test += Tests.Argument("-oDF"),
    // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
    // -a Show stack traces and exception class name for AssertionErrors.
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
    scalafmtOnCompile := true,
    headerLicense := Some(HeaderLicense.Custom("Copyright (C) 2018 Albert Serrallé"))
  )
}

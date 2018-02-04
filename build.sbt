import com.typesafe.sbt.SbtNativePackager._
import com.typesafe.sbt.packager.SettingsHelper._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.Docker
import scala.sys.process._

val shortCommit = ("git rev-parse --short HEAD" !!).replaceAll("\\n", "").replaceAll("\\r", "")

lazy val commonSettings = Seq(
  organization := "org.elmarweber.github.bigquery",
  version := s"1.0.0-${shortCommit}",
  scalaVersion := "2.12.4",
  resolvers += Resolver.jcenterRepo,

  scalacOptions := Seq(
    "-encoding", "utf8",
    "-feature",
    "-unchecked",
    "-deprecation",
    "-target:jvm-1.8",
    "-Xlog-reflective-calls",
    "-Ypatmat-exhaust-depth", "40",
    "-Xmax-classfile-name", "240" // for docker container
  ),

  publishArtifact in (Compile, packageDoc) := false,

  updateOptions := updateOptions.value.withCachedResolution(true)
)


val commonDockerSettings = Seq(
  packageName in Docker := "elmarweber/" + name.value,
  dockerBaseImage       := "openjdk:8",
  defaultLinuxInstallLocation in Docker := s"/opt/${name.value}", // to have consistent directory for files
  dockerRepository := None
)



val defaultLib = Seq(
  libraryDependencies ++= {
    val akkaV            = "2.5.9"
    val akkaHttpV        = "10.0.11"
    val logbackV         = "1.1.3"
    Seq(
      "com.typesafe.akka" %% "akka-actor"                        % akkaV,
      "com.typesafe.akka" %% "akka-stream"                       % akkaV,
      "com.typesafe.akka" %% "akka-http"                         % akkaHttpV,
      "com.typesafe.akka" %% "akka-http-spray-json"              % akkaHttpV,
      "com.typesafe.akka" %% "akka-slf4j"                        % akkaV,
      "com.typesafe.scala-logging" %% "scala-logging"            % "3.7.2",
      "com.github.scopt"  %% "scopt"                             % "3.7.0",
      "ch.qos.logback"    %  "logback-classic"                   % logbackV,
      "commons-dbutils"    % "commons-dbutils"                   % "1.6",
      "org.apache.commons" % "commons-dbcp2"                     % "2.1.1",
      "mysql"             %  "mysql-connector-java"              % "8.0.8-dmr",
      "org.specs2"        %% "specs2-core"                       % "4.0.2"    % Test,
      "com.typesafe.akka" %% "akka-http-testkit"                 % akkaHttpV  % Test,
      "com.typesafe.akka" %% "akka-stream-testkit"               % akkaV      % Test,
      "io.spray"          %% "spray-json"                        % "1.3.3"
    )
  }
)

lazy val root = (project in file("."))
  .settings(Seq(name := "mqsql-bq-load-test"))
  .settings(commonSettings)
  .settings(defaultLib)
  .settings(commonDockerSettings)
  .settings(Seq(dockerUpdateLatest := true))
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
  .enablePlugins(DebianPlugin)
  .enablePlugins(JDebPackaging)





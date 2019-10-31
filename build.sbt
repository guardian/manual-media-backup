import com.typesafe.sbt.packager.docker
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.{dockerExposedPorts, dockerUsername}
import com.typesafe.sbt.packager.docker.{Cmd, DockerPermissionStrategy}

enablePlugins(DockerPlugin, AshScriptPlugin)

name := "manual-media-backup"

version := "0.1"

scalaVersion := "2.12.8"

val akkaVersion = "2.5.22"
val circeVersion = "0.9.3"
val slf4jVersion = "1.7.25"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % "10.1.7",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-java8" % circeVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "commons-codec" % "commons-codec" % "1.12",
  "commons-io" % "commons-io" % "2.6",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.github.scopt" %% "scopt" % "3.7.1",
  "org.specs2" %% "specs2-core" % "4.5.1" % Test,
  "org.specs2" %% "specs2-mock" % "4.5.1" % Test,
  "org.mockito" % "mockito-core" % "2.28.2" % Test
)


version := sys.props.getOrElse("build.number","DEV")

dockerPermissionStrategy := DockerPermissionStrategy.Run
daemonUserUid in Docker := None
daemonUser in Docker := "daemon"
dockerUsername  := sys.props.get("docker.username")
dockerRepository := Some("guardianmultimedia")
packageName in Docker := "guardianmultimedia/manual-media-backup"
packageName := "manual-media-backup"
dockerBaseImage := "openjdk:8-jdk-alpine"
dockerAlias := docker.DockerAlias(None, Some("guardianmultimedia"),"manual-media-backup",Some(sys.props.getOrElse("build.number","DEV")))
dockerCommands ++= Seq(
  Cmd("USER","root"), //fix the permissions in the built docker image
  Cmd("RUN", "chown daemon /opt/docker"),
  Cmd("RUN", "chmod u+w /opt/docker"),
  Cmd("RUN", "chmod -R a+x /opt/docker"),
  Cmd("USER", "daemon")
)
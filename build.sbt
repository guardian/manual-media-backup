name := "manual-media-backup"

version := "0.1"

scalaVersion := "2.12.8"

val akkaVersion = "2.5.22"
val circeVersion = "0.9.3"
val slf4jVersion = "1.7.25"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "commons-codec" % "commons-codec" % "1.12",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.github.scopt" %% "scopt" % "3.7.1",
  "org.specs2" %% "specs2-core" % "4.5.1" % Test,
  "org.specs2" %% "specs2-mock" % "4.5.1" % Test,
  "org.mockito" % "mockito-core" % "2.28.2" % Test
)
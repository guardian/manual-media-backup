import com.typesafe.sbt.packager.docker
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.{dockerExposedPorts, dockerPermissionStrategy, dockerUsername}
import com.typesafe.sbt.packager.docker.{Cmd, DockerPermissionStrategy}

enablePlugins(DockerPlugin, RpmPlugin, AshScriptPlugin)

name := "manual-media-backup"

version := "0.1"

scalaVersion := "2.12.8"

val akkaVersion = "2.5.23"
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

lazy val `root` = (project in file("."))
    .dependsOn(common)
    .aggregate(manualbackup,vsmediabackup, showmxschecksum, inspectoid, `push-proxies`)

lazy val `common` = (project in file("common"))
    .settings(
      aggregate in Docker := false,
      publish in Docker := {},
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream" % akkaVersion,
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion %Test,
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
      ),
      unmanagedJars in Compile += file("lib/mxsjapi.jar"),
      unmanagedJars in Compile += file("lib/mxsjapi.jar"),
    )

lazy val `manualbackup` = (project in file("manual-media-backup")).enablePlugins(DockerPlugin,AshScriptPlugin)
  .dependsOn(common)
    .settings(
      version := sys.props.getOrElse("build.number","DEV"),
      dockerPermissionStrategy := DockerPermissionStrategy.Run,
      daemonUserUid in Docker := None,
      daemonUser in Docker := "daemon",
      dockerUsername  := sys.props.get("docker.username"),
      packageName in Docker := "guardianmultimedia/manual-media-backup",
      packageName := "manual-media-backup",
      dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"manual-media-backup",Some(sys.props.getOrElse("build.number","DEV"))),
      dockerBaseImage := "openjdk:14-jdk-alpine",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
        "org.specs2" %% "specs2-core" % "4.5.1" % Test,
        "org.specs2" %% "specs2-mock" % "4.5.1" % Test,
        "org.mockito" % "mockito-core" % "2.28.2" % Test
      ),
      dockerBaseImage := "openjdk:8-jdk-alpine",
      dockerCommands ++= Seq(
        Cmd("USER","root"), //fix the permissions in the built docker image
        Cmd("RUN", "chown daemon /opt/docker"),
        Cmd("RUN", "chmod u+w /opt/docker"),
        Cmd("RUN", "chmod -R a+x /opt/docker"),
        Cmd("USER", "daemon")
      )
    )

lazy val `inspectoid` = (project in file("inspect-oid")).enablePlugins(DockerPlugin,AshScriptPlugin)
  .dependsOn(common)
  .settings(
    version := sys.props.getOrElse("build.number","DEV"),
    dockerPermissionStrategy := DockerPermissionStrategy.Run,
    daemonUserUid in Docker := None,
    daemonUser in Docker := "daemon",
    dockerUsername  := sys.props.get("docker.username"),
    packageName in Docker := "guardianmultimedia/inspect-oid",
    packageName := "inspect-oid",
    dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"inspectoid",Some(sys.props.getOrElse("build.number","DEV"))),
    dockerBaseImage := "openjdk:14-jdk-alpine",
    dockerCommands ++= Seq(
      Cmd("USER","root"), //fix the permissions in the built docker image
      Cmd("RUN", "chown daemon /opt/docker"),
      Cmd("RUN", "chmod u+w /opt/docker"),
      Cmd("RUN", "chmod -R a+x /opt/docker"),
      Cmd("USER", "daemon")
    )
  )

lazy val `vsmediabackup` = (project in file("vs-media-backup")).enablePlugins(DockerPlugin, AshScriptPlugin)
  .dependsOn(common)
  .settings(
    version := sys.props.getOrElse("build.number","DEV"),
    dockerPermissionStrategy := DockerPermissionStrategy.Run,
    daemonUserUid in Docker := None,
    daemonUser in Docker := "daemon",
    dockerUsername  := sys.props.get("docker.username"),
    packageName in Docker := "guardianmultimedia/vs-media-backup",
    packageName := "vs-media-backup",
    dockerBaseImage := "openjdk:14-jdk-alpine",
    dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"vs-media-backup",Some(sys.props.getOrElse("build.number","DEV"))),
    dockerCommands ++= Seq(
      Cmd("USER","root"), //fix the permissions in the built docker image
      Cmd("RUN", "chown daemon /opt/docker && chmod u+w /opt/docker && chmod -R a+x /opt/docker"),
      Cmd("USER", "daemon")
    ),
    libraryDependencies ++=Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % "10.1.7",
      "javax.xml.bind" % "jaxb-api" % "2.3.1",       //AWS SDK complains about missing this (unlisted optional dependency)
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
      "org.mockito" % "mockito-core" % "2.28.2" % Test,
      "com.softwaremill.sttp" %% "core" % "0.0.20",
      "com.softwaremill.sttp" %% "async-http-client-backend-future" % "0.0.20",
      "org.asynchttpclient" % "async-http-client" % "2.0.37",
      "com.softwaremill.sttp" %% "akka-http-backend" % "0.0.20",
      "org.scala-lang.modules" %% "scala-xml" % "1.0.5",
    )
  )

lazy val `push-proxies` = (project in file("push-proxies")).enablePlugins(DockerPlugin, AshScriptPlugin)
  .dependsOn(common)
  .settings(
    version := sys.props.getOrElse("build.number","DEV"),
    dockerPermissionStrategy := DockerPermissionStrategy.Run,
    daemonUserUid in Docker := None,
    daemonUser in Docker := "daemon",
    dockerUsername  := sys.props.get("docker.username"),
    packageName in Docker := "guardianmultimedia/push-proxies",
    packageName := "push-proxies",
    dockerBaseImage := "openjdk:14-jdk-alpine",
    dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"push-proxies",Some(sys.props.getOrElse("build.number","DEV"))),
    dockerCommands ++= Seq(
      Cmd("USER","root"), //fix the permissions in the built docker image
      Cmd("RUN", "chown daemon /opt/docker && chmod u+w /opt/docker && chmod -R a+x /opt/docker"),
      Cmd("USER", "daemon")
    ),
    rpmRelease := "1",
    rpmVendor := "Guardian Multimedia <guardianmultimedia@theguardian.com>",
    libraryDependencies ++=Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % "10.1.7",
      "com.lightbend.akka" %% "akka-stream-alpakka-s3" % "1.1.2",
      "com.amazonaws" % "aws-java-sdk-s3" % "1.11.372",
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
      "org.mockito" % "mockito-core" % "2.28.2" % Test,
      "org.scala-lang.modules" %% "scala-xml" % "1.0.5",
      "com.gu" %% "akka-vidispine-components" % "0.2"
    )
  )

lazy val `showmxschecksum` = (project in file("show-mxs-checksum")).enablePlugins(DockerPlugin, AshScriptPlugin)
  .dependsOn(common)
  .settings(
    version := sys.props.getOrElse("build.number","DEV"),
    dockerPermissionStrategy := DockerPermissionStrategy.Run,
    daemonUserUid in Docker := None,
    daemonUser in Docker := "daemon",
    dockerUsername  := sys.props.get("docker.username"),
    packageName in Docker := "guardianmultimedia/show-mxs-checksum",
    packageName := "show-mxs-checksum",
    dockerBaseImage := "openjdk:14-jdk-alpine",
    dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"show-mxs-checksum",Some(sys.props.getOrElse("build.number","DEV"))),
    dockerCommands ++= Seq(
      Cmd("USER","root"), //fix the permissions in the built docker image
      Cmd("RUN", "chown daemon /opt/docker"),
      Cmd("RUN", "chmod u+w /opt/docker"),
      Cmd("RUN", "chmod -R a+x /opt/docker"),
      Cmd("USER", "daemon")
    ),
    libraryDependencies ++=Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % "10.1.7",
      "commons-codec" % "commons-codec" % "1.12",
      "commons-io" % "commons-io" % "2.6",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "org.specs2" %% "specs2-core" % "4.5.1" % Test,
      "org.specs2" %% "specs2-mock" % "4.5.1" % Test,
      "org.mockito" % "mockito-core" % "2.28.2" % Test,
    )
  )

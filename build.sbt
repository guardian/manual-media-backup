import com.typesafe.sbt.packager.docker
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.{dockerExposedPorts, dockerPermissionStrategy, dockerUsername}
import com.typesafe.sbt.packager.docker.{Cmd, DockerPermissionStrategy}

enablePlugins(DockerPlugin, RpmPlugin, AshScriptPlugin)

name := "manual-media-backup"

version := "0.1"

scalaVersion := "2.12.8"

val akkaVersion = "2.5.31"
val circeVersion = "0.9.3"
val slf4jVersion = "1.7.25"
val awsVersion = "1.11.954"

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
    .aggregate(manualbackup, showmxschecksum, inspectoid, `backup-estimate-tool`, removearchived)

lazy val `common` = (project in file("common"))
    .settings(
      aggregate in Docker := false,
      publish in Docker := {},
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream" % akkaVersion,
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion %Test,
        "com.typesafe.akka" %% "akka-http" % "10.1.7",
        "eu.medsea.mimeutil" % "mime-util" % "2.1.3" exclude("org.slf4j","slf4j-log4j12"),
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

lazy val `findfilename` = (project in file("find-filename"))
  .dependsOn(common)
  .settings(
    unmanagedJars in Compile += file("lib/mxsjapi.jar"),
    unmanagedJars in Compile += file("lib/mxsjapi.jar"),
  )

lazy val `backup-estimate-tool` = (project in file("backup-estimate-tool")).enablePlugins(DockerPlugin, AshScriptPlugin)
  .dependsOn(common)
  .settings(
    version := sys.props.getOrElse("build.number","DEV"),
    mappings in Universal ++= Seq(
      (baseDirectory.value / "../upload-estimate/upload-estimate") -> "utils/upload-estimate"
    ),
    dockerPermissionStrategy := DockerPermissionStrategy.CopyChown,
    daemonUserUid in Docker := None,
    daemonUser in Docker := "daemon",
    dockerUsername  := sys.props.get("docker.username"),
    packageName in Docker := "guardianmultimedia/manual-media-backup",
    packageName := "manual-media-backup",
    dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"backup-estimate-tool",Some(sys.props.getOrElse("build.number","DEV"))),
    libraryDependencies ++= Seq(
      "eu.medsea.mimeutil" % "mime-util" % "2.1.3" exclude("org.slf4j","slf4j-log4j12"),
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "org.specs2" %% "specs2-core" % "4.5.1" % Test,
      "org.specs2" %% "specs2-mock" % "4.5.1" % Test,
      "org.mockito" % "mockito-core" % "2.28.2" % Test
    ),
    dockerBaseImage := "openjdk:8-jdk-slim",
    dockerCommands ++= Seq(
      Cmd("COPY", "/opt/docker/utils/upload-estimate", "/opt/docker/utils/upload-estimate"),
      Cmd("USER","root"),
    )
  )

lazy val `manualbackup` = (project in file("manual-media-backup")).enablePlugins(DockerPlugin,AshScriptPlugin)
  .dependsOn(common)
    .settings(
      version := sys.props.getOrElse("build.number","DEV"),
      mappings in Universal ++= Seq(
        (baseDirectory.value / "../upload-estimate/upload-estimate") -> "utils/upload-estimate"
      ),
      dockerPermissionStrategy := DockerPermissionStrategy.CopyChown,
      daemonUserUid in Docker := None,
      daemonUser in Docker := "daemon",
      dockerUsername  := sys.props.get("docker.username"),
      packageName in Docker := "guardianmultimedia/manual-media-backup",
      packageName := "manual-media-backup",
      dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"manual-media-backup",Some(sys.props.getOrElse("build.number","DEV"))),
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
        "org.specs2" %% "specs2-core" % "4.5.1" % Test,
        "org.specs2" %% "specs2-mock" % "4.5.1" % Test,
        "org.mockito" % "mockito-core" % "2.28.2" % Test
      ),
      dockerBaseImage := "openjdk:8-jdk-slim",
      dockerCommands ++= Seq(
        Cmd("COPY", "/opt/docker/utils/upload-estimate", "/opt/docker/utils/upload-estimate"),
        Cmd("USER","root"),
      )
    )

lazy val `directcopy` = (project in file("directcopy")).enablePlugins(DockerPlugin,AshScriptPlugin)
  .dependsOn(common)
  .settings(
    version := sys.props.getOrElse("build.number","DEV"),
    dockerPermissionStrategy := DockerPermissionStrategy.CopyChown,
    daemonUserUid in Docker := None,
    daemonUser in Docker := "daemon",
    dockerUsername  := sys.props.get("docker.username"),
    packageName in Docker := "guardianmultimedia/mmb-directcopy",
    packageName := "mmb-directcopy",
    dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"mmb-directcopy",Some(sys.props.getOrElse("build.number","DEV"))),
    dockerBaseImage := "openjdk:8-jdk-slim",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "org.specs2" %% "specs2-core" % "4.5.1" % Test,
      "org.specs2" %% "specs2-mock" % "4.5.1" % Test,
      "org.mockito" % "mockito-core" % "2.28.2" % Test
    ),
    dockerBaseImage := "openjdk:8-jdk-slim",
    dockerCommands ++= Seq(
      Cmd("USER","root"),
    )
  )

lazy val `inspectoid` = (project in file("inspect-oid")).enablePlugins(DockerPlugin,AshScriptPlugin)
  .dependsOn(common)
  .settings(
    version := sys.props.getOrElse("build.number","DEV"),
    dockerPermissionStrategy := DockerPermissionStrategy.CopyChown,
    daemonUserUid in Docker := None,
    daemonUser in Docker := "daemon",
    dockerUsername  := sys.props.get("docker.username"),
    packageName in Docker := "guardianmultimedia/inspect-oid",
    packageName := "inspect-oid",
    dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"inspectoid",Some(sys.props.getOrElse("build.number","DEV"))),
    dockerBaseImage := "openjdk:8-jdk-slim",
    dockerCommands ++= Seq(
      Cmd("USER","root"), //fix the permissions in the built docker image
      Cmd("RUN", "chown daemon /opt/docker"),
      Cmd("RUN", "chmod u+w /opt/docker"),
      Cmd("RUN", "chmod -R a+x /opt/docker"),
      Cmd("USER", "daemon")
    )
  )

lazy val `showmxschecksum` = (project in file("show-mxs-checksum")).enablePlugins(DockerPlugin, AshScriptPlugin)
  .dependsOn(common)
  .settings(
    version := sys.props.getOrElse("build.number","DEV"),
    dockerPermissionStrategy := DockerPermissionStrategy.CopyChown,
    daemonUserUid in Docker := None,
    daemonUser in Docker := "daemon",
    dockerUsername  := sys.props.get("docker.username"),
    packageName in Docker := "guardianmultimedia/show-mxs-checksum",
    packageName := "show-mxs-checksum",
    dockerBaseImage := "openjdk:8-jdk-slim",
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

lazy val `removearchived` = (project in file("remove-archived")).enablePlugins(DockerPlugin, AshScriptPlugin)
  .dependsOn(common)
  .settings(
    version := sys.props.getOrElse("build.number","DEV"),
    dockerPermissionStrategy := DockerPermissionStrategy.CopyChown,
    daemonUserUid in Docker := None,
    daemonUser in Docker := "daemon",
    dockerUsername  := sys.props.get("docker.username"),
    packageName in Docker := "guardianmultimedia/remove-archived",
    packageName := "remove-archived",
    dockerBaseImage := "openjdk:8-alpine",
    dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"remove-archived",Some(sys.props.getOrElse("build.number","DEV"))),
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
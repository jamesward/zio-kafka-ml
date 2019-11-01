enablePlugins(JavaAppPackaging)

name := "zio-kafka-ml"

scalaVersion := "2.12.10"

val ZioVersion = "1.0.0-RC16"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % ZioVersion,
  "dev.zio" %% "zio-streams" % ZioVersion,
  "dev.zio" %% "zio-kafka" % "0.3.2",
  "com.softwaremill.sttp.client" %% "core" % "2.0.0-M7",
  "com.softwaremill.sttp.client" %% "async-http-client-backend-zio" % "2.0.0-M7",
  //"com.softwaremill.sttp.client" %% "async-http-client-backend-zio-streams" % "2.0.0-M7",
  "org.slf4j" % "slf4j-simple" % "1.7.26",
)

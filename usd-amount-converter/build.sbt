scalaVersion := "2.12.13"

name := "usd-amount-converter"
organization := "fraudio.com"
version := "0.0.1"


libraryDependencies ++= Seq(
  //"org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
  "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided", // Supported by EMR 6.2.0
  "com.softwaremill.sttp.client3" %% "core" % "3.3.12",
  "com.softwaremill.sttp.client3" %% "async-http-client-backend-future" % "3.3.12",
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
)

val circeVersion = "0.14.1"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)


parallelExecution in test := false
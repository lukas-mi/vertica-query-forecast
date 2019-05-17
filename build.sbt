name := "vertica-query-forecast"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http"            % "10.1.8",
  "com.typesafe.akka" %% "akka-stream"          % "2.5.22",
  "io.spray"          %% "spray-json"           % "1.3.5",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.8",
  "com.github.scopt"  %% "scopt"                % "4.0.0-RC2",
  "org.log4s"         %% "log4s"                % "1.7.0",
  "ch.qos.logback"    % "logback-classic"       % "1.2.3",
  "org.scalatest"     %% "scalatest"            % "3.0.7" % "test"
)

mainClass in assembly := Some("org.mikelionis.lukas.vqf.Main")

test in assembly := {}

assemblyJarName in assembly := "vertica-query-forecast.jar"

name := "open_images_downloader"

version := "1.0"

scalaVersion := "2.12.2"

val akkaVersion = "2.4.19"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-http-core" % "10.0.9"
libraryDependencies += "org.rogach" %% "scallop" % "3.0.3" // command line parsing
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "0.10" // akka streams csv parsing
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.14" // for tar file parsing/extraction
libraryDependencies += "com.jsuereth" %% "scala-arm" % "2.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.3" % Test
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test
// for testing image libraries
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.11" % Test


// for configuring logging to a file
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"


enablePlugins(JavaAppPackaging)
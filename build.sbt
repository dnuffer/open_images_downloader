name := "open_images_downloader"

version := "1.0"

scalaVersion := "2.12.2"

val akkaVersion = "2.4.19"

libraryDependencies += "org.scalatest" % "scalatest_2.12" % "3.0.3" % "test"
libraryDependencies += "com.typesafe.akka" % "akka-actor_2.12" % akkaVersion
libraryDependencies += "com.typesafe.akka" % "akka-testkit_2.12" % akkaVersion % "test"
libraryDependencies += "com.typesafe.akka" % "akka-http-core_2.12" % "10.0.9"
libraryDependencies += "org.rogach" %% "scallop" % "3.0.3" // command line parsing
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "0.10"

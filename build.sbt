name := "open_images_downloader"

version := "1.0"

scalaVersion := "2.12.2"

val akkaVersion = "2.4.19"

resolvers += "OpenMicroscopy Repository" at "http://artifacts.openmicroscopy.org/artifactory/ome.releases/"
resolvers += "ImageJ" at "http://maven.imagej.net/content/repositories/releases/"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.3" % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-http-core" % "10.0.9"
libraryDependencies += "org.rogach" %% "scallop" % "3.0.3" // command line parsing
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "0.10"
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.14" // for tar file parsing/extraction
libraryDependencies += "com.jsuereth" %% "scala-arm" % "2.0"

// image libraries
//libraryDependencies += "ome" % "turbojpeg" % "5.6.0-m2"
//libraryDependencies += "idr" % "turbojpeg" % "0.4.1"
//libraryDependencies += "ome" % "turbojpeg" % "5.5.3"
//libraryDependencies += "loci" % "turbojpeg" % "5.0.0-beta1"

libraryDependencies += "org.imgscalr" % "imgscalr-lib" % "4.2"
libraryDependencies += "com.twelvemonkeys.imageio" % "imageio" % "3.3.2"
libraryDependencies += "com.twelvemonkeys.imageio" % "imageio-core" % "3.3.2"
libraryDependencies += "com.twelvemonkeys.imageio" % "imageio-jpeg" % "3.3.2"

//libraryDependencies += "jmagick" % "jmagick" % "6.6.9" % "provided"
libraryDependencies += "org.im4java" % "im4java" % "1.4.0"

// for testing image libraries
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.11"

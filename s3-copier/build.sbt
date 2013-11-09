name := "intelanalytics-s3-copier"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.3"

resolvers += "sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "commons-codec" % "commons-codec" % "1.8",
  "org.mockito" % "mockito-core" % "1.9.5",
  "com.amazonaws" % "aws-java-sdk" % "1.6.4",
  "com.github.seratch" % "awscala_2.10" % "0.1.0-SNAPSHOT",
  "org.apache.hadoop" % "hadoop-client" % "1.2.1",
  "com.typesafe.play" %% "play-json" % "2.2.0",
  "com.github.scala-incubator.io" % "scala-io-file_2.10" % "0.4.2",
  "com.github.scopt" %% "scopt" % "3.1.0",
  "org.scalaz" %% "scalaz-core" % "7.0.4"
)     





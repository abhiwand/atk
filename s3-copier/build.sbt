name := "intelanalytics-s3-copier"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.3"

resolvers += "sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"


libraryDependencies ++= Seq(
  "commons-codec" % "commons-codec" % "1.8",
  "org.mockito" % "mockito-core" % "1.9.5",
  "com.amazonaws" % "aws-java-sdk" % "1.6.4",
  "com.github.seratch" % "awscala_2.10" % "0.1.0-SNAPSHOT"
)     





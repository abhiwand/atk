name := "tribeca-web"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  jdbc,
  "mysql" % "mysql-connector-java" % "5.1.21",
  anorm,
  cache,
  "com.typesafe.play" %% "play-slick" % "0.5.0.8",
  "commons-codec" % "commons-codec" % "1.8",
  "org.mockito" % "mockito-core" % "1.9.5",
  "com.amazonaws" % "aws-java-sdk" % "1.6.4"
)     

play.Project.playScalaSettings

org.scalastyle.sbt.ScalastylePlugin.Settings


//name in Rpm := "intelanalytics-saas-web",
   // version in Rpm <<= sbtVersion.identity,
   // rpmRelease := "1",
  //  rpmVendor := "Intel",
   // rpmUrl := None,
    //rpmLicense := None,



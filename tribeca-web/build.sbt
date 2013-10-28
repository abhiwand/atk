name := "tribeca-web"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  jdbc,
  "mysql" % "mysql-connector-java" % "5.1.21",
  anorm,
  cache,
  "com.typesafe.play" %% "play-slick" % "0.5.0.8"
)     

play.Project.playScalaSettings


name := "tribeca-web"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  jdbc,
  "mysql" % "mysql-connector-java" % "5.1.18",
  anorm,
  cache
)     

play.Project.playScalaSettings


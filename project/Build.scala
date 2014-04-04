import sbt._
import Keys._

object IABuild extends Build {

  lazy val root = Project(id = "intel-analytics",
                          base = file(".")) dependsOn (launcher, server, engine)

  lazy val launcher = Project(id = "launcher", base = file("launcher"))

  lazy val server = Project(id = "api-server",
                         base = file("api-server")) dependsOn (interfaces, shared)

  lazy val engine = Project(id = "engine",
                         base = file("engine")) dependsOn (interfaces, shared, launcher)

  lazy val shared = Project(id = "shared",
              base = file("shared"))

  lazy val interfaces = Project(id = "interfaces",
              base = file("interfaces"))

  lazy val spark = Project(id = "spark", base = file("spark")) dependsOn (interfaces)

  val akkaV = "2.3.0"
  val sprayV = "1.3.1"
  val slickV = "2.0.1"
}

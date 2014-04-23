import sbt._
import Keys._
import sbtassembly.Plugin.Assembly
import com.zavakid.sbt.OneLog.OneLogKeys._

object IABuild extends Build {

  lazy val root = Project(id = "root",
                          base = file("."))
                          .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
                          .settings(oneLogSettings: _*)
                          .aggregate(event, /* graphbuilder,*/ interfaces, launcher, server, engine, spark)
                          .dependsOn(shared)

  lazy val launcher = Project(id = "launcher", base = file("launcher"))
                          .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
                          .settings(oneLogSettings: _*)

  lazy val server = Project(id = "api-server",
                         base = file("api-server"))
                        .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
                        .settings(oneLogSettings: _*)
                        .dependsOn (interfaces, shared, launcher)

  lazy val engine = Project(id = "engine",
                         base = file("engine"))
                        .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
                        .settings(oneLogSettings: _*)
                        .dependsOn (interfaces, shared, launcher)

  lazy val shared = Project(id = "shared",
                      base = file("shared"))
                      .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
                      .settings(oneLogSettings: _*)
                      .dependsOn (interfaces, event)

  lazy val interfaces = Project(id = "interfaces",
                          base = file("interfaces"))
                        .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
                        .settings(oneLogSettings: _*)

  lazy val spark = Project(id = "engine-spark", base = file("engine-spark"))
                    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
                    .settings(oneLogSettings: _*)
                    .dependsOn (interfaces, event, shared)

//  lazy val graphbuilder = Project(id = "graphbuilder", base = file("graphbuilder-3"))
//                    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)

  lazy val event = Project(id = "event", base = file("event"))
                    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
                    .settings(oneLogSettings: _*)

  val akkaV = "2.3.2"
  val sprayV = "1.3.1"
  val slickV = "2.0.1"
}

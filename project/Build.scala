
import sbt._
import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

//import com.zavakid.sbt.OneLog.OneLogKeys._

object IABuild extends Build {

  val commonSettings =
    net.virtualvoid.sbt.graph.Plugin.graphSettings ++
      scalariformSettings ++
      //oneLogSettings
      List(
        ScalariformKeys.preferences := ScalariformKeys.preferences.value
          .setPreference(AlignParameters, true)
          .setPreference(CompactControlReadability, true)
      )

  lazy val root = Project(id = "root", base = file("."))
    .settings(commonSettings: _*)
    .aggregate(event, isparkdeps, graphbuilder, interfaces, launcher, server, engine, spark, shared)

  lazy val launcher = Project(id = "launcher", base = file("launcher"))
    .settings(commonSettings: _*)

  lazy val server = Project(id = "api-server", base = file("api-server"))
    .settings(commonSettings: _*)
    .dependsOn(interfaces, shared, launcher)

  lazy val engine = Project(id = "engine", base = file("engine"))
    .settings(commonSettings: _*)
    .dependsOn(interfaces, shared, launcher)

  lazy val shared = Project(id = "shared", base = file("shared"))
    .settings(commonSettings: _*)
    .dependsOn(interfaces, event)

  lazy val interfaces = Project(id = "interfaces", base = file("interfaces"))
    .settings(commonSettings: _*)

  lazy val spark = Project(id = "engine-spark", base = file("engine-spark"))
    .settings(commonSettings: _*)
    .dependsOn(interfaces, event, shared)

  lazy val graphbuilder = Project(id = "graphbuilder-3", base = file("graphbuilder-3"))
    .settings(commonSettings: _*) dependsOn (isparkdeps)

  lazy val isparkdeps = Project(id = "ispark-deps", base = file("ispark-deps"))
    .settings(commonSettings: _*)

  lazy val event = Project(id = "event", base = file("event"))
    .settings(commonSettings: _*)

  val akkaV = "2.3.2"
  val sprayV = "1.3.1"
  val sprayJsonV = "1.2.5"
  val slickV = "2.0.1"
}

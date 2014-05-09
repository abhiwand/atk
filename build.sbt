import spray.revolver.RevolverPlugin.Revolver

organization in ThisBuild := "com.intel"

version in ThisBuild      := "0.8"

scalaVersion in ThisBuild := "2.10.3"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers in ThisBuild ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  Resolver.mavenLocal,
  DefaultMavenRepository
)


libraryDependencies in ThisBuild ++= {
  Seq(
    "org.scala-lang"      %   "scala-reflect"     % scalaVersion.value,
    "io.spray"            %%  "spray-json"        % sprayJsonV,
    "org.specs2"          %%  "specs2-core"       % "2.3.10"  % "test",
    "org.specs2"          %%  "specs2-mock"       % "2.3.10"  % "test",
    "org.specs2"          %%  "specs2-html"       % "2.3.10"  % "test",
    "org.specs2"          %%  "specs2-scalacheck" % "2.3.10"  % "test",
    "org.mockito"         %   "mockito-core"      % "1.9.5"   % "test",
    "org.specs2"	%% 	"specs2"	 % "2.3.10"   % "test",
//    ("com.intel.hadoop"    %   "event"             % "1.0-SNAPSHOT")
//      .exclude("ch.qos.cal10n.plugins", "maven-cal10n-plugin")
//      .exclude("junit", "junit")
//      .exclude("org.apache.hadoop", "hadoop-core"),
    "ch.qos.logback"      %   "logback-classic"   % "1.1.1",
    "org.slf4j"           %   "slf4j-api"         % "1.7.5",
    "com.jsuereth"        %%  "scala-arm"         % "1.3"
  )
}

seq(Revolver.settings: _*)

net.virtualvoid.sbt.graph.Plugin.graphSettings

org.scalastyle.sbt.ScalastylePlugin.Settings


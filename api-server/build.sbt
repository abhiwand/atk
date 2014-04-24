import spray.revolver.RevolverPlugin.Revolver

organization  := "com.intel"

version       := "0.1"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  Resolver.mavenLocal
)

libraryDependencies ++= {
  Seq(
    "org.scala-lang"      %   "scala-reflect"     % scalaVersion.value,
    "io.spray"            %   "spray-can"         % sprayV,
    "io.spray"            %   "spray-routing"     % sprayV,
    "io.spray"            %%  "spray-json"        % "1.2.5",
    "io.spray"            %   "spray-testkit"     % sprayV    % "test",
    "com.typesafe.akka"   %%  "akka-actor"        % akkaV,
    "com.typesafe.akka"   %%  "akka-slf4j"        % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"      % akkaV     % "test",
    "com.typesafe.slick"  %%  "slick"             % "2.0.1-RC1",
    "com.h2database"      %   "h2"                % "1.3.175",
//    ("com.intel.hadoop"    %   "event"             % "1.0-SNAPSHOT")
//      .exclude("ch.qos.cal10n.plugins", "maven-cal10n-plugin")
//      .exclude("junit", "junit")
//      .exclude("org.apache.hadoop", "hadoop-core"),
    "ch.qos.logback"      %   "logback-classic"   % "1.1.1",
    "org.slf4j"           %   "slf4j-api"         % "1.7.6"
    //"com.gettyimages"     %%  "spray-swagger"     % "0.3.1"
  )
}

seq(Revolver.settings: _*)

net.virtualvoid.sbt.graph.Plugin.graphSettings
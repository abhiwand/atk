libraryDependencies := {
  Seq("org.scala-lang"      %   "scala-library"     % scalaVersion.value,
      "io.spray"            %%  "spray-json"        % sprayJsonV,
      "joda-time"           %   "joda-time"         % "2.3"
  )
}

net.virtualvoid.sbt.graph.Plugin.graphSettings

ScoverageSbtPlugin.instrumentSettings
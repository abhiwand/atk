libraryDependencies := {
  Seq("org.scala-lang"      %   "scala-library"     % scalaVersion.value,
      "io.spray"            %%  "spray-json"        % sprayJsonV)
}

net.virtualvoid.sbt.graph.Plugin.graphSettings
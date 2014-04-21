libraryDependencies := {
  Seq("org.scala-lang"      %   "scala-library"     % scalaVersion.value,
      "org.scala-lang"      %   "scala-reflect"  % scalaVersion.value)
}

net.virtualvoid.sbt.graph.Plugin.graphSettings
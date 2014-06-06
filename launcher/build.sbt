libraryDependencies := {
  Seq("org.scala-lang"      %   "scala-library"     % scalaVersion.value,
      "org.scala-lang"      %   "scala-reflect"     % scalaVersion.value,
      "org.scalatest"       %%  "scalatest"         % "2.1.6" % "test"
  )
}

net.virtualvoid.sbt.graph.Plugin.graphSettings

ScoverageSbtPlugin.instrumentSettings

import sbt.ExclusionRule

//Set the dependencies with := instead of appending with ++= since we don't want the
//default ones from the parent for this module.
libraryDependencies := {
  Seq("org.scala-lang" % "scala-library" % scalaVersion.value,
      ("org.apache.spark" %% "spark-core" % "0.9.0-incubating").
        exclude("org.mortbay.jetty", "servlet-api").
        exclude("commons-beanutils", "commons-beanutils-core").
        exclude("commons-collections", "commons-collections").
        exclude("commons-collections", "commons-collections").
        exclude("com.esotericsoftware.minlog", "minlog").
        exclude("org.slf4j", "slf4j-log4j12").
        exclude("ch.qos.logback", "logback-classic"))
}


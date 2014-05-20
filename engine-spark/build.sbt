import sbt.ExclusionRule

//Set the dependencies with := instead of appending with ++= since we don't want the
//default ones from the parent for this module.
libraryDependencies := {
  Seq("org.scala-lang" % "scala-library" % scalaVersion.value,
      ("org.apache.spark" %% "spark-core" % "0.9.1").
        exclude("org.mortbay.jetty", "servlet-api").
        exclude("commons-beanutils", "commons-beanutils-core").
        exclude("commons-collections", "commons-collections").
        exclude("commons-collections", "commons-collections").
        exclude("com.esotericsoftware.minlog", "minlog").
        exclude("org.slf4j", "slf4j-log4j12").
        exclude("ch.qos.logback", "logback-classic"),
  "com.jsuereth"        %%  "scala-arm"         % "1.3",
	"org.specs2"        %%	"specs2"         % "2.3.10"   % "test",
    ("org.apache.hadoop" % "hadoop-client" % "2.2.0")
      .exclude("asm", "asm")
      .exclude("org.slf4j", "slf4j-log4j12"),
    ("org.apache.hadoop" % "hadoop-hdfs" % "2.2.0")
      .exclude("asm", "asm")
      .exclude("org.slf4j", "slf4j-log4j12"),
    ("org.apache.hadoop" % "hadoop-common" % "2.2.0")
      .exclude("asm", "asm")
      .exclude("org.slf4j", "slf4j-log4j12")
  )
}

ScoverageSbtPlugin.instrumentSettings

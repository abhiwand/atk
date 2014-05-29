import sbt.{DefaultMavenRepository, Resolver, ExclusionRule}

resolvers ++= Seq(
  Resolver.sonatypeRepo("snapshots"),
  Resolver.sonatypeRepo("releases"),
  Resolver.typesafeRepo("releases"),
  "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  DefaultMavenRepository,
  Resolver.mavenLocal
  //"gao-mirror" at "http://gaomaven.jf.intel.com:8081/nexus/content/groups/public"
)


//Set the dependencies with := instead of appending with ++= since we don't want the
//default ones from the parent for this module.
libraryDependencies := {
  Seq("org.scala-lang" % "scala-library" % scalaVersion.value,
// graphbuilder likes this version of spark-core
      ("org.apache.spark" %% "spark-core" % "0.9.0-cdh5.0.0").
//        exclude("org.mortbay.jetty", "servlet-api").
//        exclude("commons-beanutils", "commons-beanutils-core").
//        exclude("commons-collections", "commons-collections").
//        exclude("commons-collections", "commons-collections").
        exclude("com.esotericsoftware.minlog", "minlog").
        exclude("org.slf4j", "slf4j-log4j12").
        exclude("ch.qos.logback", "logback-classic"),
  "com.jsuereth"        %%  "scala-arm"         % "1.3",
	"org.specs2"        %%	"specs2"         % "2.3.10"   % "test",
  "org.scalatest"     %%  "scalatest"      % "2.1.6"    % "test",
    ("org.apache.hadoop" % "hadoop-client" % "2.2.0")
      .exclude("asm", "asm")
      .exclude("org.slf4j", "slf4j-log4j12"),
    ("org.apache.hadoop" % "hadoop-hdfs" % "2.2.0")
      .exclude("asm", "asm")
      .exclude("org.slf4j", "slf4j-log4j12"),
//    ("org.apache.hadoop" % "hadoop-hdfs" % "2.3.0-cdh5.0.0")
//      .exclude("asm", "asm")
//      .exclude("org.slf4j", "slf4j-log4j12"),
    ("org.apache.hadoop" % "hadoop-common" % "2.3.0-cdh5.0.0")
      .exclude("asm", "asm")
      .exclude("org.slf4j", "slf4j-log4j12")
  )
}

ScoverageSbtPlugin.instrumentSettings

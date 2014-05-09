
import sbt.Keys._

import sbtassembly.Plugin.AssemblyKeys._

name := "graphbuilder-3"

version := "3"

scalaVersion := "2.10.3"

libraryDependencies := {
  Seq(
    // ispark-deps provides hbase-protocol
    "com.intel.bda" % "ititan-core" % "0.4.5-SNAPSHOT", // Titan core to get around Kryo ClassLoader issue
    "com.thinkaurelius.titan" % "titan-berkeleyje" % "0.4.5-SNAPSHOT" exclude("com.thinkaurelius.titan", "titan-core"),
    "com.thinkaurelius.titan" % "titan-cassandra" % "0.4.5-SNAPSHOT" excludeAll ExclusionRule(organization = "org.jboss.netty") exclude("com.thinkaurelius.titan", "titan-core"),
    "com.thinkaurelius.titan" % "titan-hbase" % "0.4.5-SNAPSHOT" excludeAll ExclusionRule(organization = "org.apache.hbase") exclude("com.thinkaurelius.titan", "titan-core") exclude("javax.servlet.jsp", "jsp-api"),
    "org.apache.commons" % "commons-lang3" % "3.3.1",
    "org.apache.hadoop" % "hadoop-mapreduce-client-app" % "2.3.0-cdh5.0.0",
    "org.apache.hbase" % "hbase-client" % "0.96.1.1-cdh5.0.0",
    "org.apache.hbase" % "hbase-common" % "0.96.1.1-cdh5.0.0",
    "org.apache.hbase" % "hbase-server" % "0.96.1.1-cdh5.0.0" exclude("org.eclipse.jdt", "core"),
    "org.apache.spark" % "spark-core_2.10" % "0.9.0-cdh5.0.0",
    "org.specs2" %% "specs2" % "2.3.10" % "test",
    "org.scalacheck" %% "scalacheck" % "1.11.3" % "test"//,
    //"com.thinkaurelius.titan" % "titan-core" % "0.4.5-SNAPSHOT" % "provided"  exclude("org.slf4j", "slf4j-log4j12") intransitive(),
    //"org.apache.hbase" % "hbase-protocol" % "0.96.1.1-cdh5.0.0" % "provided" intransitive()
  )
}

// These are the direct dependencies for titan-core 0.4.2 excluding Kryo to get around ClassLoader issue
// See ititan project
libraryDependencies ++= Seq(
  "com.tinkerpop.blueprints" % "blueprints-core" % "2.4.0",
  "com.tinkerpop" % "frames" % "2.4.0",
  "com.codahale.metrics" % "metrics-core" % "3.0.1", // "3.0.0-BETA3",
  "com.codahale.metrics" % "metrics-ganglia" % "3.0.1", //"3.0.0-BETA3",
  "com.codahale.metrics" % "metrics-graphite" % "3.0.1", //"3.0.0-BETA3",
  "com.spatial4j" % "spatial4j" % "0.3",
  "commons-collections" % "commons-collections" % "3.2.1",
  "commons-configuration" % "commons-configuration" % "1.6",
  "commons-io" % "commons-io" % "2.1",
  "commons-codec" % "commons-codec" % "1.7",
  "com.google.guava" % "guava" % "14.0.1",
  "com.google.code.findbugs" % "jsr305" % "1.3.9",
  "com.carrotsearch" % "hppc" % "0.4.2",
  "com.github.stephenc.high-scale-lib" % "high-scale-lib" % "1.1.4"
  // shaded "com.esotericsoftware.kryo" % "kryo" % "2.21",
)

// From Specs2 WIKI
scalacOptions in Test ++= Seq("-Yrangepos")

// See https://github.com/jrudolph/sbt-dependency-graph
net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= Seq(
  Resolver.sonatypeRepo("snapshots"),
  Resolver.sonatypeRepo("releases"),
  Resolver.typesafeRepo("releases"),
  "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  DefaultMavenRepository,
  Resolver.mavenLocal
  //"gao-mirror" at "http://gaomaven.jf.intel.com:8081/nexus/content/groups/public"
)

ScoverageSbtPlugin.instrumentSettings

ScoverageSbtPlugin.ScoverageKeys.excludedPackages in ScoverageSbtPlugin.scoverage := "com.intel.graphbuilder.driver.spark.titan.examples.*;com.intel.graphbuilder.driver.local.examples.*"

org.scalastyle.sbt.ScalastylePlugin.Settings
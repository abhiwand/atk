
import sbt.Keys._

import sbtassembly.Plugin.AssemblyKeys._

import com.typesafe.sbt.SbtScalariform._

import scalariform.formatter.preferences._

name := "graphbuilder-3"

version := "3"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.intel.bda" % "ititan-core" % "0.4.2-SNAPSHOT", // Titan core to get around Kryo ClassLoader issue
  "com.thinkaurelius.titan" % "titan-berkeleyje" % "0.4.2" exclude("com.thinkaurelius.titan", "titan-core"),
  "com.thinkaurelius.titan" % "titan-cassandra" % "0.4.2" excludeAll ExclusionRule(organization = "org.jboss.netty") exclude("com.thinkaurelius.titan", "titan-core"),
  // Titan requires HBase 0.94 but CDH comes with HBase 0.96
  //"com.thinkaurelius.titan" % "titan-hbase" % "0.4.2" excludeAll( ExclusionRule(organization = "org.apache.hbase") ),
  "org.apache.commons" % "commons-lang3" % "3.3.1",
  "org.apache.hadoop" % "hadoop-mapreduce-client-app" % "2.3.0-cdh5.0.0",
  "org.apache.hbase" % "hbase-client" % "0.96.1.1-cdh5.0.0",
  "org.apache.spark" % "spark-core_2.10" % "0.9.0-cdh5.0.0"
)

// These are the direct dependencies for titan-core 0.4.2 excluding Kryo to get around ClassLoader issue
// See ititan project
libraryDependencies ++= Seq(
  "com.tinkerpop.blueprints" % "blueprints-core" % "2.4.0",
  "com.tinkerpop" % "frames" % "2.4.0",
  "com.codahale.metrics" % "metrics-core" % "3.0.0-BETA3",
  "com.codahale.metrics" % "metrics-ganglia" % "3.0.0-BETA3",
  "com.codahale.metrics" % "metrics-graphite" % "3.0.0-BETA3",
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

libraryDependencies ++= Seq(
    "org.specs2" %% "specs2" % "2.3.10" % "test",
    "org.scalacheck" %% "scalacheck" % "1.11.3" % "test"
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
//  "gao-mirror" at "http://gaomaven.jf.intel.com:8081/nexus/content/groups/public",
  Resolver.mavenLocal
)

ScoverageSbtPlugin.instrumentSettings

ScoverageSbtPlugin.ScoverageKeys.excludedPackages in ScoverageSbtPlugin.scoverage := "com.intel.graphbuilder.driver.spark.titan.examples.*;com.intel.graphbuilder.driver.local.examples.*"

org.scalastyle.sbt.ScalastylePlugin.Settings

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignParameters, true)
  .setPreference(CompactControlReadability, true)

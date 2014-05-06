
import sbt.Keys._

import sbtassembly.Plugin.AssemblyKeys._

import com.typesafe.sbt.SbtScalariform._

import scalariform.formatter.preferences._

name := "graphbuilder-3"

version := "3"

scalaVersion := "2.10.4"

libraryDependencies := {
  Seq(
    // ispark-deps provides titan-core and hbase-protocol
    "com.thinkaurelius.titan" % "titan-berkeleyje" % "0.4.5-SNAPSHOT" exclude("com.thinkaurelius.titan", "titan-core") exclude("org.slf4j", "slf4j-log4j12"),
    "com.thinkaurelius.titan" % "titan-cassandra" % "0.4.5-SNAPSHOT" excludeAll ExclusionRule(organization = "org.jboss.netty") exclude("com.thinkaurelius.titan", "titan-core") exclude("org.slf4j", "slf4j-log4j12"),
    "com.thinkaurelius.titan" % "titan-hbase" % "0.4.5-SNAPSHOT" excludeAll ExclusionRule(organization = "org.apache.hbase") exclude("com.thinkaurelius.titan", "titan-core") exclude("javax.servlet.jsp", "jsp-api") exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.commons" % "commons-lang3" % "3.3.1"  exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.hadoop" % "hadoop-mapreduce-client-app" % "2.3.0-cdh5.0.0"  exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.hbase" % "hbase-client" % "0.96.1.1-cdh5.0.0" exclude("org.slf4j", "slf4j-log4j12"), // % "provided",
    "org.apache.hbase" % "hbase-common" % "0.96.1.1-cdh5.0.0" exclude("org.slf4j", "slf4j-log4j12"), // % "provided",
    "org.apache.hbase" % "hbase-server" % "0.96.1.1-cdh5.0.0" exclude("org.eclipse.jdt", "core") exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.spark" % "spark-core_2.10" % "0.9.0-cdh5.0.0" exclude("org.slf4j", "slf4j-log4j12")
  )
}

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
  DefaultMavenRepository,
  Resolver.mavenLocal,
  "gao-mirror" at "http://gaomaven.jf.intel.com:8081/nexus/content/groups/public"
)

ScoverageSbtPlugin.instrumentSettings

ScoverageSbtPlugin.ScoverageKeys.excludedPackages in ScoverageSbtPlugin.scoverage := "com.intel.graphbuilder.driver.spark.titan.examples.*;com.intel.graphbuilder.driver.local.examples.*"

org.scalastyle.sbt.ScalastylePlugin.Settings

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignParameters, true)
  .setPreference(CompactControlReadability, true)

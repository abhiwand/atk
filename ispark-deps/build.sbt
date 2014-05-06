
//  This project is for dependencies that should be installed in Spark using SPARK_CLASSPATH.
//
//  The number of dependencies listed here should be minimized.
//
//  Some dependencies need to be installed via SPARK_CLASSPATH because of =ClassLoader conflicts
//  when providing them via sparkContext.addJars("")
//
//  Build with "sbt assembly"
//

organization  := "com.intel"

version       := "0.1"

scalaVersion  := "2.10.3"

resolvers ++= Seq(
  Resolver.sonatypeRepo("snapshots"),
  Resolver.sonatypeRepo("releases"),
  Resolver.typesafeRepo("releases"),
  "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  DefaultMavenRepository,
  Resolver.mavenLocal
)

libraryDependencies := Seq(
  // Titan core to get around Kryo ClassLoader issue
  "com.thinkaurelius.titan" % "titan-core" % "0.4.5-SNAPSHOT",
  // Classloader issue with com.google.protobuf
  "org.apache.hbase" % "hbase-protocol" % "0.96.1.1-cdh5.0.0"
)

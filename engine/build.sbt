
libraryDependencies ++= {
  Seq(
    "org.scala-lang"      %   "scala-reflect"     % scalaVersion.value,
    "com.typesafe.akka"   %%  "akka-actor"        % akkaV,
    "com.typesafe.akka"   %%  "akka-slf4j"        % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"      % akkaV     % "test",
    //Workflow engine things:
    "org.apache.spark"    %%  "spark-core"        % "0.9.0-incubating" % "provided"
    //"commons-httpclient"  %   "commons-httpclient"% "3.1"
  )
}


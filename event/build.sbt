libraryDependencies ++= {
  Seq(("org.apache.hadoop"           %   "hadoop-core"       % "1.0.4" % "provided"),
      "com.google.guava"            %   "guava"             % "11.0.2",
      "commons-httpclient"          %   "commons-httpclient"% "3.1",
      "com.googlecode.json-simple"  %   "json-simple"       % "1.1.1")
}

net.virtualvoid.sbt.graph.Plugin.graphSettings
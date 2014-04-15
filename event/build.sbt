

libraryDependencies ++= {
  Seq(("org.apache.hadoop"          %   "hadoop-core"             % "1.0.4" % "provided"),
      "com.google.guava"            %   "guava"                   % "11.0.2",
      "commons-httpclient"          %   "commons-httpclient"      % "3.1",
      "com.googlecode.json-simple"  %   "json-simple"             % "1.1.1",
      "org.powermock"               %   "powermock-module-junit4" % "1.5.1" % "test",
      "org.powermock"               %   "powermock-api-mockito"   % "1.5.1" % "test",
      "junit"                       %   "junit"                   % "1.5.1" % "test",
      "org.hamcrest"                %   "hamcrest-library"        % "1.3" % "test",
      "org.apache.mrunit"           %   "mrunit"                  % "0.9.0-incubating" % "test" classifier "hadoop1",
      "com.novocode"                %   "junit-interface"         % "0.9" % "test"
  )
}

net.virtualvoid.sbt.graph.Plugin.graphSettings
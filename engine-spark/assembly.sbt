import AssemblyKeys._ // put this at the top of the file

assemblySettings

// your assembly settings here

jarName in assembly := "engine-spark.jar"

test in assembly := {}

assemblyOption in assembly ~= { _.copy(cacheOutput = false) }

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
    case PathList("javax", "xml", xs @ _*)             => MergeStrategy.first
    case PathList("org", "apache", xs @ _*)            => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case "com/esotericsoftware/minlog/Log$Logger.class" => MergeStrategy.first
    case "com/esotericsoftware/minlog/Log.class" => MergeStrategy.first
    case PathList("log4j.properties") => MergeStrategy.first
    case PathList("StaticLoggerBinder")  => MergeStrategy.first
    case x => old(x)
  }
}




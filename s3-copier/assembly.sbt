import AssemblyKeys._ // put this at the top of the file

assemblySettings

// your assembly settings here

jarName in assembly := "s3copier.jar"

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
    case PathList("javax", "xml", xs @ _*)             => MergeStrategy.first
    case PathList("org", "apache", xs @ _*)             => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case x => old(x)
  }
}



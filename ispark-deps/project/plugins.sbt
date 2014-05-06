
resolvers ++= Seq(
  Resolver.typesafeRepo("releases"),
  Resolver.sonatypeRepo("releases"),
  Classpaths.sbtPluginReleases
)

// Create one big jar
addSbtPlugin("com.eed3si9n" %% "sbt-assembly" % "0.10.1")

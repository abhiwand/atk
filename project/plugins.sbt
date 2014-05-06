addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.1")

resolvers ++= Seq(
    Resolver.typesafeRepo("releases"),
    Resolver.sonatypeRepo("releases"),
    Classpaths.sbtPluginReleases,
    DefaultMavenRepository
)

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.5.2")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.3.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.10.1")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

addSbtPlugin("com.orrsella" % "sbt-sublime" % "1.0.9")

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.98.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.2.0")

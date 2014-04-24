
resolvers ++= Seq(
  Resolver.typesafeRepo("releases"),
  Resolver.sonatypeRepo("releases"),
  Classpaths.sbtPluginReleases
)

// Generate IntelliJ project files. I didn't have good luck with this --Todd
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.3.2")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

// Create one big jar
addSbtPlugin("com.eed3si9n" %% "sbt-assembly" % "0.10.1") // version with imllib
//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2") // latest as of April 2014

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.98.0")

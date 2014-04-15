
// The Typesafe repository
resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "sonatype-releases" at "http://oss.sonatype.org/content/repositories/releases/"
)

// generate IntelliJ project files
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.3.2")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

// create one big jar
addSbtPlugin("com.eed3si9n" %% "sbt-assembly" % "0.10.1") // version with imllib
//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2") // latest as of April 2014

resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.98.0")

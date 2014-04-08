addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.1")

// The Typesafe repository 
resolvers ++= Seq(
    "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    "sonatype-releases" at "http://oss.sonatype.org/content/repositories/releases/"
    )

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.5.2")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.3.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.10.1")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

addSbtPlugin("com.orrsella" % "sbt-sublime" % "1.0.9")
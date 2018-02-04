addSbtPlugin("com.typesafe.sbt" %% "sbt-native-packager" % "1.3.3")

// fast turnaround / restart app
addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")

// jdeb packaging
libraryDependencies += "org.vafer" % "jdeb" % "1.3" artifacts (Artifact("jdeb", "jar", "jar"))

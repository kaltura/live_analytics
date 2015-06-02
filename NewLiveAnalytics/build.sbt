import sbt.Package.ManifestAttributes

name := "live-analytics-driver"

val projectVersion = sys.props.getOrElse("projectVersion", default = "1.0")
val buildVersion = sys.props.getOrElse("buildVersion", default = "0")

version := projectVersion + "." + buildVersion

organization := "kaltura"

scalaVersion := "2.10.5"

retrieveManaged := true

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "." + artifact.extension
}

packageOptions := Seq(ManifestAttributes(
  ("Implementation-Version", projectVersion + "." + buildVersion),
  ("Specification-Version", projectVersion)))

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test",
  "org.apache.spark" %% "spark-core" % "1.2.2",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.2.2",
  "eu.inn" %% "binders-cassandra" % "0.2.5"
)

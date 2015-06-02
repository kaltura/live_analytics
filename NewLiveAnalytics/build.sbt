name := "live-analytics-driver"

version :=  sys.props.getOrElse("projectVersion", default = "1.0") + "." + sys.props.getOrElse("buildVersion", default = "0")

organization := "kaltura"

scalaVersion := "2.10.4"

mainClass in assembly := Some("com.kaltura.live.MainDriver")

retrieveManaged := true

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test",
  "org.apache.spark" %% "spark-core" % "1.2.2",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.2.2",
  "eu.inn" %% "binders-cassandra" % "0.2.5"
)

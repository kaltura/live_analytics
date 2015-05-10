name := "NewLiveAnalytics"

version := "1.0"

organization := "kaltura"

scalaVersion := "2.10.4"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.2"

//libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.0"

//libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.1"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.2.2"

//libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.2.0-rc3" // for writerBuilder TTL but SomeColumns fail!!!

libraryDependencies += "eu.inn" %% "binders-cassandra" % "0.2.5"


//libraryDependencies += "kaltura" %% "ip-2-location" % "1.0.0"











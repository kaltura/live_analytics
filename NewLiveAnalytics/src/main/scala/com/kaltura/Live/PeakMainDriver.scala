package com.kaltura.Live

import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.kaltura.Live.MainDriver._
import com.kaltura.Live.infra.ConfigurationManager
import com.kaltura.Live.model.aggregation.processors.PeakAudienceNewProcessor
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
 * Created by orlylampert on 7/13/16.
 */
object PeakMainDriver {
  val jarDependenciesLocal: List[String] = List.empty

  val jarDependencies: List[String] = List(
    "live-analytics-driver.jar",
    "spark-cassandra-connector_2.10-1.2.0.jar",
    "binders-cassandra_2.10-0.2.5.jar",
    "binders-core_2.10-0.2.3.jar",
    "cassandra-driver-core-2.1.5.jar",
    "cassandra-thrift-2.1.3.jar",
    "joda-time-2.3.jar",

    // for spark 1.2.0
    "netty-3.9.0.Final.jar",
    "guava-16.0.1.jar",
    "metrics-core-3.0.2.jar",
    "slf4j-api-1.6.4.jar",
    "jsr166e-1.1.0.jar")

  val keyspace = "kaltura_live"

  val baseFieldsList  = List(
    "event_time",
    "alive",
    "dvr_alive",
    "bitrate",
    "bitrate_count",
    "buffer_time",
    "plays")

  val entryFieldsList  = baseFieldsList :+ "entry_id"

  val entryPeakFieldsList = List(
    "entry_id",
    "event_time",
    "audience",
    "dvr_audience",
    "update_time")

  val entryHourlyPeakTableName = "live_entry_hourly_peak"
  val entryHourlyPeakTableFields = toSomeColumns(entryPeakFieldsList)


  var shouldBreak = false
  var gracefullyDone = false

  def isEmpty[T](rdd : RDD[T]) = {
    rdd.take(1).size == 0
  }

  def appVersion = Resources.toString(getClass.getResource("/VERSION"), Charsets.UTF_8)

  def processEvents( sc : SparkContext ): Unit =
  {
    while (!shouldBreak) {
      val now: DateTime = new DateTime()
      PeakAudienceNewProcessor.process(sc, now.getMillis)
    }
  }

  def setShutdownHook = {
    sys.ShutdownHookThread {
      shouldBreak = true
      while(!gracefullyDone) {
        println("Waiting for current aggregation iteration to complete gracefully...")
        Thread.sleep(3000)
      }
      println("Live Analytics exited gracefully!")
    }
  }

  def main(args: Array[String])
  {

    val conf = new SparkConf()
      .setMaster(ConfigurationManager.get("spark.master"))
      .setAppName("NewLiveAnalyticsPeak")
      .set("spark.executor.memory", ConfigurationManager.get("spark.executor_memory", "8g"))
      .set("spark.cassandra.connection.host", ConfigurationManager.get("cassandra.node_name"))

    val sc = new SparkContext(conf)

    setShutdownHook

    //for ( jarDependency <- jarDependencies )
    //  sc.addJar(ConfigurationManager.get("repository_home") + "/" + jarDependency)

    println( "******************************************************")
    println(s"*************** Live Analytics v${appVersion} ****************")
    println( "******************************************************")


    processEvents(sc)
    gracefullyDone = true
  }

}

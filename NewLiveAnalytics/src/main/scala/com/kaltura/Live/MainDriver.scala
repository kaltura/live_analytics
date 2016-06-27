package com.kaltura.Live

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.kaltura.Live.infra.{ConfigurationManager, EventsGenerator}
import com.kaltura.Live.model.LiveEvent
import com.kaltura.Live.model.aggregation.processors.PeakAudienceProcessor
import com.kaltura.Live.model.purge.DataCleaner
import com.kaltura.Live.utils.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


/**
 * Created by didi on 2/23/15.
 */

// TODO: peak audience design how should it work without a counter!!!!
// TODO: For the bufferTime which is double could not be a counter so multiply by 100 and make it long
// TODO: write data to Kafka
// TODO: TTL management, check that hash for the key tuples and equal is not needed
// TODO: data validation e.g. check bufferTime <= 0 if not override bitrate >=0 bitrate <= ~TBD etc...
object MainDriver
{
     def toSomeColumns( columnNames: List[String] ) : SomeColumns =
     {
          SomeColumns(columnNames.map(x => new ColumnName(x) ): _*)
     }

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

     val entryLocationFieldsList = entryFieldsList :+ "city" :+ "country"

     val referrerFieldsList = entryFieldsList :+ "referrer"

     val partnerFieldsList = baseFieldsList :+ "partner_id"

     val entryPeakFieldsList = List(
          "entry_id",
          "event_time",
          "audience",
          "dvr_audience")

     val partnerEntryFieldsList = List(
          "partner_id",
          "entry_id",
          "event_time")

     val entryTableName = "live_events"

     //val entryTableColumnFields = SomeColumns( entryFieldsList.map(x => new ColumnName(x) ): _*)
     val entryTableColumnFields = toSomeColumns(entryFieldsList)

     val entryHourlyTableName = "hourly_live_events"
     val entryHourlyTableFields = entryTableColumnFields

     val locationEntryTableName = "live_events_location"
     val locationEntryTableFields = toSomeColumns(entryLocationFieldsList)

     val referrerHourlyTableName = "hourly_live_events_referrer"
     val referrerHourlyTableFields = toSomeColumns(referrerFieldsList)

     val partnerHourlyTableName = "hourly_live_events_partner"
     val partnerHourlyTableFields = toSomeColumns(partnerFieldsList)

     val entryHourlyPeakTableName = "live_entry_hourly_peak"
     val entryHourlyPeakTableFields = toSomeColumns(entryPeakFieldsList)

     val livePartnerEntryTableName = "live_partner_entry"
     val livePartnerEntryTableFields = toSomeColumns(partnerEntryFieldsList)

     var shouldBreak = false
     var gracefullyDone = false

     def isEmpty[T](rdd : RDD[T]) = {
          rdd.take(1).size == 0
     }

     def appVersion = Resources.toString(getClass.getResource("/VERSION"), Charsets.UTF_8)

     def processEvents( sc : SparkContext, events: RDD[LiveEvent] ): Unit =
     {
          val reducedLiveEvents = events
               .map(event => ( (event.entryId, event.eventTime), event) )
               .reduceByKey(_ + _)

          reducedLiveEvents.cache()

          reducedLiveEvents.map(x => x._2.wrap)
               .saveToCassandra(keyspace, entryTableName, entryTableColumnFields)

          PeakAudienceProcessor.process(sc, reducedLiveEvents)

          reducedLiveEvents.unpersist()

          //val temp11 = reducedLiveEvents.foreach(print(_))

          val temp2 = events.map(event => ( (event.entryId, DateUtils.roundTimeToHour(event.eventTime) ), event.roundTimeToHour ) )
               .reduceByKey(_ + _)
               .map(x => x._2.wrap)
               .saveToCassandra(keyspace, entryHourlyTableName, entryHourlyTableFields)

          val temp3 = events.map(event => ( (event.entryId, event.eventTime, event.country, event.city), event) )
               .reduceByKey(_ + _)
               .map(x => x._2.wrap)
               .saveToCassandra(keyspace, locationEntryTableName, locationEntryTableFields)

          val temp4 = events.map(event => ( (event.entryId, DateUtils.roundTimeToHour(event.eventTime), event.referrer), event.roundTimeToHour) )
               .reduceByKey(_ + _)
               .map(x => x._2.wrap)
               .saveToCassandra(keyspace, referrerHourlyTableName, referrerHourlyTableFields)

          val temp5 = events.map(event => ( (event.partnerId, DateUtils.roundTimeToHour(event.eventTime) ), event.roundTimeToHour) )
               .reduceByKey(_ + _)
               .map(x => x._2.wrap)
               .saveToCassandra(keyspace, partnerHourlyTableName, partnerHourlyTableFields)

          val temp7 = events.map(event => (event.entryId, event.roundTimeToHour) )
               .reduceByKey(_ maxTime _)
               .map(x => x._2.wrap)
               .saveToCassandra(keyspace, livePartnerEntryTableName, livePartnerEntryTableFields, writeConf = WriteConf(ttl = TTLOption.constant(36 * 60 * 60)))
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
            .setAppName("NewLiveAnalytics")
            .set("spark.executor.memory", ConfigurationManager.get("spark.executor_memory", "8g"))
            .set("spark.cassandra.connection.host", ConfigurationManager.get("cassandra.node_name"))

          val sparkSerializer = ConfigurationManager.get("spark.serializer", "default")
          if (!sparkSerializer.equals("default"))
            conf.set("spark.serializer", sparkSerializer)
          val sc = new SparkContext(conf)

          setShutdownHook

          for ( jarDependency <- jarDependencies )
               sc.addJar(ConfigurationManager.get("repository_home") + "/" + jarDependency)

          println( "******************************************************")
          println(s"*************** Live Analytics v${appVersion} ****************")
          println( "******************************************************")


          // events are returned with 10sec resolution!!!
          val eventsGenerator = new EventsGenerator(sc, ConfigurationManager.get("aggr.max_files_per_cycle", "50").toInt)
          val dataCleaner = new DataCleaner(sc)
          while (!shouldBreak) {
            val events = eventsGenerator.get
            val noEvents = isEmpty(events)
            eventsGenerator.commit
            if (!noEvents) {
              processEvents(sc, events)
            }
            dataCleaner.tryRun()
            if (noEvents) {
              Thread.sleep(1000)
            }
          }

          eventsGenerator.close
          gracefullyDone = true
     }
}
// TODO: for the peak audience read data from Cassandra for the current hour (what if we are running after crash)
// TODO: union with the new events and reduce by key when reduce is max function
// TODO: write back to Cassandra

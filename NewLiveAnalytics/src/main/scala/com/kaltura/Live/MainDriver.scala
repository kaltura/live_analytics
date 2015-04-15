package com.kaltura.Live

import com.kaltura.Live.env.EnvParams
import com.kaltura.Live.infra.EventsGenerator
import com.kaltura.Live.model.LiveEvent
import com.kaltura.Live.model.aggregation.processors.PeakAudienceProcessor
import com.kaltura.Live.utils.DateUtils
import org.apache.spark.rdd.RDD

import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

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
     val jarDependencies: List[String] = List(
          "newliveanalytics.jar",
          "spark-cassandra-connector_2.10-1.1.1.jar",
          "binders-cassandra_2.10-0.2.5.jar",
          "binders-core_2.10-0.2.3.jar",
          "cassandra-driver-core-2.1.3.jar",
          "cassandra-thrift-2.1.2.jar",
          "joda-time-2.3.jar")

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
     val entryTableColumnFields = SomeColumns(entryFieldsList: _*)

     val entryHourlyTableName = "hourly_live_events"
     val entryHourlyTableFields = entryTableColumnFields

     val locationEntryTableName = "live_events_location"
     val locationEntryTableFields = SomeColumns(entryLocationFieldsList: _*)

     val referrerHourlyTableName = "hourly_live_events_referrer"
     val referrerHourlyTableFields = SomeColumns(referrerFieldsList: _*)

     val partnerHourlyTableName = "hourly_live_events_partner"
     val partnerHourlyTableFields = SomeColumns(partnerFieldsList: _*)

     val entryHourlyPeakTableName = "live_entry_hourly_peak"
     val entryHourlyPeakTableFields = SomeColumns(entryPeakFieldsList: _*)

     val livePartnerEntryTableName = "live_partner_entry"
     val livePartnerEntryTableFields = SomeColumns(partnerEntryFieldsList: _*)


     // TODO: need to implement the following to get some signal from outside for stopping the driver
     def checkBreakRequest(): Boolean = false

     def isEmpty[T](rdd : RDD[T]) = {
          rdd.take(1).size == 0
     }

     def processEvents( sc : SparkContext, events: RDD[LiveEvent] ): Unit =
     {
          val reducedLiveEvents = events
               .map(event => ( (event.entryId, event.eventTime, event.partnerId), event) )
               .reduceByKey(_ + _)

          reducedLiveEvents.persist()

          reducedLiveEvents.map(x => x._2.wrap)
               .saveToCassandra(keyspace, entryTableName, entryTableColumnFields)

          PeakAudienceProcessor.process(sc, reducedLiveEvents)

          reducedLiveEvents.unpersist()

          //val temp11 = reducedLiveEvents.foreach(print(_))

          val temp2 = events.map(event => ( (event.entryId, DateUtils.roundTimeToHour(event.eventTime), event.partnerId), event.roundTimeToHour ) )
               .reduceByKey(_ + _)
               .map(x => x._2.wrap)
               .saveToCassandra(keyspace, entryHourlyTableName, entryHourlyTableFields)

          val temp3 = events.map(event => ( (event.entryId, event.eventTime, event.partnerId, event.country, event.city), event) )
               .reduceByKey(_ + _)
               .map(x => x._2.wrap)
               .saveToCassandra(keyspace, locationEntryTableName, locationEntryTableFields)

          val temp4 = events.map(event => ( (event.entryId, DateUtils.roundTimeToHour(event.eventTime), event.partnerId, event.referrer), event.roundTimeToHour) )
               .reduceByKey(_ + _)
               .map(x => x._2.wrap)
               .saveToCassandra(keyspace, referrerHourlyTableName, referrerHourlyTableFields)

          val temp5 = events.map(event => ( (event.partnerId, DateUtils.roundTimeToHour(event.eventTime) ), event.roundTimeToHour) )
               .reduceByKey(_ + _)
               .map(x => x._2.wrap)
               .saveToCassandra(keyspace, partnerHourlyTableName, partnerHourlyTableFields)

          val temp7 = events.map(event => ( (event.partnerId, event.entryId), event.roundTimeToHour) )
               .reduceByKey(_ maxTime _)
               .map(x => x._2.wrap)
               .saveToCassandra(keyspace, livePartnerEntryTableName, livePartnerEntryTableFields)
     }

     def main(args: Array[String])
     {
          System.setProperty("spark.default.parallelism", EnvParams.sparkParallelism)
          System.setProperty("spark.cores.max", EnvParams.sparkMaxCores)
          System.setProperty("spark.executor.memory", EnvParams.sparkExecutorMem)

          // TODO: get properties from configuration file
          val conf = new SparkConf()
               //.setMaster("spark://il-bigdata-1.dev.kaltura.com:7077")
//               .setMaster("spark://localhost:7077")
//               .setMaster("local[4]")
               .setMaster(EnvParams.sparkAddress)
               .setAppName("NewLiveAnalytics")
               .set("spark.executor.memory", "1g")
               .set("spark.cassandra.connection.host", "192.168.31.91")

          val sc = new SparkContext(conf)

          for ( jarDependency <- jarDependencies )
               sc.addJar(EnvParams.repositoryHome + "/" + jarDependency)

          // events are returned with 10sec resolution!!!
          val eventsGenerator = new EventsGenerator(sc, EnvParams.maxProcessFilesPerCycle)

          breakable
          {
               while (true)
               {
                    val events = eventsGenerator.get

                    val noEvents = isEmpty(events)

                    if ( !noEvents )
                         processEvents(sc, events)

                    eventsGenerator.commit

                    if ( noEvents )
                         Thread.sleep(1000)
               }
          }

          eventsGenerator.close
     }
}
// TODO: for the peak audience read data from Cassandra for the current hour (what if we are running after crash)
// TODO: union with the new events and reduce by key when reduce is max function
// TODO: write back to Cassandra

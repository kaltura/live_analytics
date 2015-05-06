package com.kaltura.Live.model.aggregation.processors

import com.kaltura.Live.MainDriver
import com.kaltura.Live.infra.SerializedSession
import com.kaltura.Live.model.LiveEvent
import com.kaltura.Live.model.dao.{EntryHourlyPeakAudience, EntryHourlyPeakAudienceCF}
import com.kaltura.Live.utils.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

import com.datastax.spark.connector._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}

object PeakAudienceProcessor
{
     val keyspace = MainDriver.keyspace
     val tableName = MainDriver.entryHourlyPeakTableName

     // for cassandra version above 2.1.X
//     def processPro( events: RDD[LiveEvent] ): Unit =
//     {
//          events.map(event => ( (event.entryId, DateUtils.roundTimeToHour(event.eventTime), event.partnerId), event.roundTimeToHour) )
//               .reduceByKey(_ max _)
//               //.map(x => x._2.wrap)
//               .map(x => x._2)
//               .map( savePro(_) )
//     }

     // for cassandra version above 2.1.X
//     def savePro( event: LiveEvent ): (Future[Unit], Future[Unit], Future[Unit]) =
//     {
//          val entryHourlyPeakAudienceCF = new EntryHourlyPeakAudienceCF(SerializedSession.session)
//
//          val insertIfNotExist = entryHourlyPeakAudienceCF.insertIfNotExist(event.entryId, event.eventTime, event.alive, event.dvrAlive)
//
//          val updateAudienceResult = entryHourlyPeakAudienceCF.updateAudienceIfGreater(event.entryId, event.eventTime, event.alive)
//          val updateDVRAudienceResult = entryHourlyPeakAudienceCF.updateDVRAudienceIfGreater(event.entryId, event.eventTime, event.dvrAlive)
//
//          val ret = (updateAudienceResult, updateDVRAudienceResult, insertIfNotExist)
//
//          ret
//     }

     def process( sc : SparkContext, reducedEvents: RDD[( (String, Long), LiveEvent) ] ): Unit =
     {
          // create all distinct (entry_id, event_time(hourly) ) tuple and then select using cassandraTable with case class all the needed old events
          // this time without the need to use java.util.Date but Long
          // cassandraTable has select Where...

          /*val hours = newAudienceEvents.map(x => x._1._2)
              //.map( DateUtils.roundTimeToHour(_) ) // not needed again
              .distinct()
              .collect()
              .toList*/

          //          val oldAudience = Await.result(entryHourlyPeakAudienceCF.getAudienceByHours(hours), scala.concurrent.duration.Duration.Inf)
          //               .toList

          val newAudienceEvents = reducedEvents.map(x => ( (x._1._1, DateUtils.roundTimeToHour(x._1._2) ), x._2.roundTimeToHour ) )
               .reduceByKey(_ max _)

          newAudienceEvents.persist()

          //val oldAudienceEvents = loadValues(sc, newAudienceEvents.keys) // TODO return this line instead of the next one if possible to serialize sc.cassandraTable!!!
          val oldAudienceEvents = loadValuesOtherMethod(newAudienceEvents.keys)


          val newAudienceKeyEvents = newAudienceEvents.map(x => ( x._1, (new EntryHourlyPeakAudience(x._2.entryId, new java.util.Date(x._2.eventTime), x._2.alive, x._2.dvrAlive) ) ) )

          val oldAudienceKeyEvents = oldAudienceEvents.keyBy(x => (x.entry_id, x.event_time.getTime) )

          val coGroupedAudience = oldAudienceKeyEvents.cogroup(newAudienceKeyEvents)

          //coGroupedAudience.count()

          coGroupedAudience.filter(x => saveIfNewOrGreater(x._2._1, x._2._2) )
               .map(x => x._2._2.head)
               .saveToCassandra(keyspace, tableName)
     }

     def loadValues(  sc : SparkContext, keys: RDD[( (String, Long)) ] ) : RDD[EntryHourlyPeakAudience] =
     {
          val values = keys.flatMap(x =>
               sc.cassandraTable[EntryHourlyPeakAudience](keyspace, tableName)
                    .where("entry_id = ?", x._1)
                    .where("event_time = ?", x._2)
                    .collect()
                    .toList)

          values
     }

     def loadValuesOtherMethod(  keys: RDD[( (String, Long)) ] ) : RDD[EntryHourlyPeakAudience] =
     {
          //val entryHourlyPeakAudienceCF = new EntryHourlyPeakAudienceCF(SerializedSession.session)

          // TODO: consider removing the wait ...
          val values = keys.flatMap(x => Await.result(EntryHourlyPeakAudienceCF.getByKey(x._1 ,new java.util.Date(x._2) ), scala.concurrent.duration.Duration.Inf) )

          values
     }


     def saveIfNewOrGreater( oldEvents: Iterable[EntryHourlyPeakAudience],
                             newEvents: Iterable[EntryHourlyPeakAudience] ): Boolean =
     {
          ( ( oldEvents.isEmpty ) || ( !newEvents.isEmpty && isNewPeakFound(oldEvents.head, newEvents.head) )  )
     }

     def isNewPeakFound( oldEvent: EntryHourlyPeakAudience, newEvent: EntryHourlyPeakAudience ): Boolean =
     {
          assert(oldEvent != null)
          assert(newEvent != null)
          
          return ( newEvent.audience > oldEvent.audience || newEvent.dvr_audience > oldEvent.dvr_audience )
     }

//     def save( event: LiveEvent ): (Future[Unit], Future[Unit], Future[Unit]) =
//     {
//          val entryHourlyPeakAudienceCF = new EntryHourlyPeakAudienceCF(SerializedSession.session)
//
//          val insertIfNotExist = entryHourlyPeakAudienceCF.insertIfNotExist(event.entryId, event.eventTime, event.alive, event.dvrAlive)
//
//          val updateAudienceResult = entryHourlyPeakAudienceCF.updateAudienceIfGreater(event.entryId, event.eventTime, event.alive)
//          val updateDVRAudienceResult = entryHourlyPeakAudienceCF.updateDVRAudienceIfGreater(event.entryId, event.eventTime, event.dvrAlive)
//
//          val ret = (updateAudienceResult, updateDVRAudienceResult, insertIfNotExist)
//
//          ret
//     }

}

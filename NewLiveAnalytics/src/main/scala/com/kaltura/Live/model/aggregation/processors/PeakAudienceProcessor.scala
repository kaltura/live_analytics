package com.kaltura.Live.model.aggregation.processors

import com.kaltura.Live.infra.SerializedSession
import com.kaltura.Live.model.LiveEvent
import com.kaltura.Live.model.dao.{EntryHourlyPeakAudience, EntryHourlyPeakAudienceCF}
import com.kaltura.Live.utils.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}

object PeakAudienceProcessor
{
     // for cassandra version above 2.1.X
     def processPro( events: RDD[LiveEvent] ): Unit =
     {
          events.map(event => ( (event.entryId, DateUtils.roundTimeToHour(event.eventTime), event.partnerId), event.roundTimeToHour) )
               .reduceByKey(_ max _)
               //.map(x => x._2.wrap)
               .map(x => x._2)
               .map( savePro(_) )
     }

     // for cassandra version above 2.1.X
     def savePro( event: LiveEvent ): (Future[Unit], Future[Unit], Future[Unit]) =
     {
          val entryHourlyPeakAudienceCF = new EntryHourlyPeakAudienceCF(SerializedSession.session)

          val insertIfNotExist = entryHourlyPeakAudienceCF.insertIfNotExist(event.entryId, event.eventTime, event.alive, event.dvrAlive)

          val updateAudienceResult = entryHourlyPeakAudienceCF.updateAudienceIfGreater(event.entryId, event.eventTime, event.alive)
          val updateDVRAudienceResult = entryHourlyPeakAudienceCF.updateDVRAudienceIfGreater(event.entryId, event.eventTime, event.dvrAlive)

          val ret = (updateAudienceResult, updateDVRAudienceResult, insertIfNotExist)

          ret
     }

     def process( sc : SparkContext, reducedEvents: RDD[( (String, Long), LiveEvent) ] ): Unit =
     {
          val newAudienceEvents = reducedEvents.map(x => ( (x._1._1, DateUtils.roundTimeToHour(x._1._2) ), x._2.roundTimeToHour ) )
               .reduceByKey(_ max _)
               .map(x => ( (x._1._1, x._1._2), (new EntryHourlyPeakAudience(x._2.entryId, new java.util.Date(x._2.eventTime), x._2.alive, x._2.dvrAlive) ) ) )

          newAudienceEvents.persist()

          val hours = newAudienceEvents.map(x => x._1._2)
               .map( DateUtils.roundTimeToHour(_) )
               .distinct()
               .collect()
               .toList

          val entryHourlyPeakAudienceCF = new EntryHourlyPeakAudienceCF(SerializedSession.session)

          val oldAudience = Await.result(entryHourlyPeakAudienceCF.getAudienceByHours(hours), scala.concurrent.duration.Duration.Inf)
               .toList

          val oldAudienceEvents = sc.parallelize(oldAudience)
               .keyBy(x => (x.entry_id, x.event_time) )

          val coGroupedAudience = oldAudienceEvents.cogroup(oldAudienceEvents)

          coGroupedAudience.persist()

          coGroupedAudience.map(x => saveIfNewOrGreater(entryHourlyPeakAudienceCF, x._2._1, x._2._2) )
     }

     def saveIfNewOrGreater( entryHourlyPeakAudienceCF: EntryHourlyPeakAudienceCF, oldEvents: Iterable[EntryHourlyPeakAudience], newEvents: Iterable[EntryHourlyPeakAudience] ): Unit =
     {
          if ( ( oldEvents.isEmpty ) ||
               ( !newEvents.isEmpty && isNewPeakFound(oldEvents.head, newEvents.head) )  )
               entryHourlyPeakAudienceCF.insert(newEvents.head)
     }

     def isNewPeakFound( oldEvent: EntryHourlyPeakAudience, newEvent: EntryHourlyPeakAudience ): Boolean =
     {
          assert(oldEvent != null)
          assert(newEvent != null)
          
          return ( newEvent.audience > oldEvent.audience || newEvent.dvr_audience > oldEvent.dvr_audience )
     }

     def save( event: LiveEvent ): (Future[Unit], Future[Unit], Future[Unit]) =
     {
          val entryHourlyPeakAudienceCF = new EntryHourlyPeakAudienceCF(SerializedSession.session)

          val insertIfNotExist = entryHourlyPeakAudienceCF.insertIfNotExist(event.entryId, event.eventTime, event.alive, event.dvrAlive)

          val updateAudienceResult = entryHourlyPeakAudienceCF.updateAudienceIfGreater(event.entryId, event.eventTime, event.alive)
          val updateDVRAudienceResult = entryHourlyPeakAudienceCF.updateDVRAudienceIfGreater(event.entryId, event.eventTime, event.dvrAlive)

          val ret = (updateAudienceResult, updateDVRAudienceResult, insertIfNotExist)

          ret
     }

}

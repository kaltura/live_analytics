package com.kaltura.Live.model.aggregation.processors

import com.kaltura.Live.infra.SerializedSession
import com.kaltura.Live.model.LiveEvent
import com.kaltura.Live.model.dao.EntryHourlyPeakAudienceCF
import com.kaltura.Live.utils.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

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

     def process( sc : SparkContext, reducedEvents: RDD[( (String, Long, Int), LiveEvent) ] ): Unit =
     {
          // get the hours to load from cassandra by reduce all unique hours
          // load all data for those hours
          // union with current events
          // reduce
          // save - maybe only those which are found new

          val newAudienceEvents = reducedEvents.map(x => ( (x._1._1, DateUtils.roundTimeToHour(x._1._2), x._1._3), x._2 ) )
               .reduceByKey(_ max _)

          newAudienceEvents.persist()

          val hours = newAudienceEvents.map(x => x._1._2)
               .map( DateUtils.roundTimeToHour(_) )
               .distinct()
               .collect()
               .toList

          val entryHourlyPeakAudienceCF = new EntryHourlyPeakAudienceCF(SerializedSession.session)

          val oldAudience = Await.result(entryHourlyPeakAudienceCF.getAudienceByHours(hours), scala.concurrent.duration.Duration.Inf)
               .toList

          val OldAudienceEvents = sc.parallelize(oldAudience)


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

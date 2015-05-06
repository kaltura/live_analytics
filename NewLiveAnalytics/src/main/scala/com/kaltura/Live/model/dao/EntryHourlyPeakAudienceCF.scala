package com.kaltura.Live.model.dao

import com.kaltura.Live.infra.SerializedSession
import com.kaltura.Live.model.{LiveEvent}
import eu.inn.binders._
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.PlainConverter
import scala.concurrent.{Future, ExecutionContext}

object EntryHourlyPeakAudience
{
     def maxCollectionsHeadsIfExists( oldEvents: Iterable[EntryHourlyPeakAudience], newEvents: Iterable[EntryHourlyPeakAudience] ): EntryHourlyPeakAudience =
     {
          if ( oldEvents.isEmpty )
               return newEvents.head

          if ( newEvents.isEmpty )
               return oldEvents.head

          return newEvents.head.max(oldEvents.head)
     }

     def isNewOrGreater( oldEvents: Iterable[EntryHourlyPeakAudience], newEvents: Iterable[EntryHourlyPeakAudience] ): Boolean =
     {
          ( ( oldEvents.isEmpty ) || ( !newEvents.isEmpty && isNewPeakFound(oldEvents.head, newEvents.head) )  )
     }

     def isNewPeakFound( oldEvent: EntryHourlyPeakAudience, newEvent: EntryHourlyPeakAudience ): Boolean =
     {
          assert(oldEvent != null)
          assert(newEvent != null)

          return ( newEvent.audience > oldEvent.audience || newEvent.dvr_audience > oldEvent.dvr_audience )
     }
}

case class EntryHourlyPeakAudience( entry_id: String, event_time: java.util.Date, audience: Long, dvr_audience: Long ) extends Serializable
{
     def max( that: EntryHourlyPeakAudience ): EntryHourlyPeakAudience =
          new EntryHourlyPeakAudience(this.entry_id, this.event_time, math.max(this.audience, that.audience),
               math.max(this.dvr_audience, that.dvr_audience) )
}

object EntryHourlyPeakAudienceCF//( session: com.datastax.driver.core.Session ) extends Serializable
{
     val session: com.datastax.driver.core.Session = SerializedSession.session

     import ExecutionContext.Implicits.global

     implicit val cache = new SessionQueryCache[PlainConverter](session)

     def insert( peakEvent: EntryHourlyPeakAudience ): Future[Unit] = cql"insert into live_entry_hourly_peak(entry_id, event_time, audience, dvr_audience) values (?, ?, ?, ?)".bind(peakEvent).execute()

     def insertIfNotExist(entry_id: String, event_time: Long/*java.util.Date*/, audience: Long, dvrAudience: Long): Future[Unit]
          = cql"UPDATE live_entry_hourly_peak SET audience=$audience AND dvr_audience=$dvrAudience WHERE entry_id=$entry_id AND event_time=$event_time IF NOT EXIST".execute()

     def updateAudienceIfGreater(entry_id: String, event_time: Long/*java.util.Date*/, audience: Long): Future[Unit]
     = cql"UPDATE live_entry_hourly_peak SET audience=$audience WHERE entry_id=$entry_id AND event_time=$event_time IF audience<$audience".execute()

     def updateDVRAudienceIfGreater(entry_id: String, event_time: Long/*java.util.Date*/, dvrAudience: Long): Future[Unit]
          = cql"UPDATE live_entry_hourly_peak SET dvr_audience=$dvrAudience WHERE entry_id=$entry_id AND event_time=$event_time IF dvr_audience<$dvrAudience".execute()

     def getByKey( entry_id: String , event_time: java.util.Date ) : Future[Option[EntryHourlyPeakAudience]] =
          cql"SELECT * FROM live_entry_hourly_peak WHERE entry_id=$entry_id AND event_time=$event_time".oneOption[EntryHourlyPeakAudience]

     def getAudienceByHours( hours: List[Long] ) : Future[Iterator[EntryHourlyPeakAudience]] =
     {
          val datesStrings : List[String] = hours.map(x => new java.util.Date(x).toString )
          val hoursCommaSeparated = datesStrings.mkString(", ")

          cql"SELECT * FROM live_entry_hourly_peak WHERE event_time IN ($hoursCommaSeparated) ALLOW FILTERING".all[EntryHourlyPeakAudience]

          //val hoursCommaSeparated = hours.mkString(", ")

          //cql"SELECT * FROM live_entry_hourly_peak WHERE event_time IN ($hoursCommaSeparated) ALLOW FILTERING".all[EntryHourlyPeakAudience]
     }



}

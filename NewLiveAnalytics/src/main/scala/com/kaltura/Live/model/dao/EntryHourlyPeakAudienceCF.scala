package com.kaltura.Live.model.dao

import com.kaltura.Live.model.{LiveEvent}
import eu.inn.binders._
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.PlainConverter
import scala.concurrent.{Future, ExecutionContext}

case class EntryHourlyPeakAudience( entry_id: String, event_time: java.util.Date, audience: Long, dvr_audience: Long ) extends Serializable

class EntryHourlyPeakAudienceCF( session: com.datastax.driver.core.Session ) extends Serializable
{
     import ExecutionContext.Implicits.global

     implicit val cache = new SessionQueryCache[PlainConverter](session)

     def insertIfNotExist(entry_id: String, event_time: Long/*java.util.Date*/, audience: Long, dvrAudience: Long): Future[Unit]
          = cql"UPDATE live_entry_hourly_peak SET audience=$audience AND dvr_audience=$dvrAudience WHERE entry_id='$entry_id' AND event_time=$event_time IF NOT EXIST".execute()

     def updateAudienceIfGreater(entry_id: String, event_time: Long/*java.util.Date*/, audience: Long): Future[Unit]
     = cql"UPDATE live_entry_hourly_peak SET audience=$audience WHERE entry_id='$entry_id' AND event_time=$event_time IF audience<$audience".execute()

     def updateDVRAudienceIfGreater(entry_id: String, event_time: Long/*java.util.Date*/, dvrAudience: Long): Future[Unit]
          = cql"UPDATE live_entry_hourly_peak SET dvr_audience=$dvrAudience WHERE entry_id='$entry_id' AND event_time=$event_time IF dvr_audience<$dvrAudience".execute()

     def getAudienceByHours( hours: List[Long] ) : Future[Iterator[EntryHourlyPeakAudience]] =
     {
          val hoursCommaSeparated = hours.mkString(", ")

          cql"SELECT * FROM live_entry_hourly_peak WHERE event_time IN ($hoursCommaSeparated)".all[EntryHourlyPeakAudience]
     }

}

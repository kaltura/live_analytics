package com.kaltura.Live.model.dao

import com.kaltura.Live.model.{LiveEvent}
import eu.inn.binders._
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.PlainConverter
import scala.concurrent.{Future, ExecutionContext}

//case class EntryHourlyPeakAudience( entry_id: String, event_time: java.util.Date, audience: Long, dvr_audience: Long ) extends Serializable

class EntryHourlyPeakAudienceCF( session: com.datastax.driver.core.Session ) extends Serializable
{
     import ExecutionContext.Implicits.global

     implicit val cache = new SessionQueryCache[PlainConverter](session)

     def updateAudience(entry_id: String, event_time: Long/*java.util.Date*/, audience: Long): Future[Unit]
          = cql"UPDATE live_entry_hourly_peak SET audience=$audience WHERE entry_id='$entry_id' AND event_time=$event_time IF audience<$audience".execute()

     def updateDVRAudience(entry_id: String, event_time: Long/*java.util.Date*/, dvrAudience: Long): Future[Unit]
          = cql"UPDATE live_entry_hourly_peak SET dvr_audience=$dvrAudience WHERE entry_id='$entry_id' AND event_time=$event_time IF NOT EXIST OR dvr_audience<$dvrAudience".execute()

}

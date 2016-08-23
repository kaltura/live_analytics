package com.kaltura.Live.model.dao

import java.util.Date

import com.kaltura.Live.infra.SerializedSession
import com.kaltura.Live.model.aggregation.processors.PeakAudience
import com.kaltura.Live.utils.DateUtils
import eu.inn.binders.cassandra._
import eu.inn.binders.naming.PlainConverter

import scala.concurrent.{Await, ExecutionContext, Future}

case class LiveEvents( entry_id: String, event_time: Date, alive: Long, dvr_alive: Long ) extends Serializable

object LiveEvents
{
     val session: com.datastax.driver.core.Session = SerializedSession.session

     implicit val cache = new SessionQueryCache[PlainConverter](session)
     import ExecutionContext.Implicits.global

     def selectEventsFrom(peak: PeakAudience) : Future[Iterator[LiveEvents]] = {
          val nextHour = new Date(DateUtils.roundTimeToHour(peak.updateTime.getTime+1000L*3600))
          val thisHour = peak.eventTime
          // start from last update time - 2 minutes to make sure that 10 sec aggregation is ready but not before the current hour
          val updateTime = latest(thisHour, new Date(peak.updateTime.getTime-(1000L*120)))

          val entryId = peak.entryId
          val res = cql"select * from kaltura_live.live_events where entry_id=$entryId and event_time >= $updateTime and event_time < $nextHour"
          res.all[LiveEvents]

     }

     def selectPeak(peak: PeakAudience) : PeakAudience = {

          val events = Await.result(selectEventsFrom(peak), scala.concurrent.duration.Duration.Inf)
          val liveEvent = LiveEvents(peak.entryId, peak.updateTime, peak.audience, peak.dvrAudience)
          val peakForTimeRange = if (events.isEmpty) {
               liveEvent
          } else {
               events.reduceLeft( (x,y) => LiveEvents(x.entry_id, latest(x.event_time, y.event_time), x.alive max y.alive,x.dvr_alive max y.dvr_alive))
          }

          PeakAudience(peak.entryId, peak.eventTime, peakForTimeRange.event_time, peak.audience max peakForTimeRange.alive, peak.dvrAudience max peakForTimeRange.dvr_alive)

     }

     def latest(a: Date, b: Date) : Date =
     {
          if (a.after(b))
               a
          else
               b
     }



}


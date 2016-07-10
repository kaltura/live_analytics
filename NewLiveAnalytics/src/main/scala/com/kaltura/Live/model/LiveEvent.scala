package com.kaltura.Live.model

import java.util.regex.Pattern

import com.kaltura.Live.utils.{BaseLog, MetaLog, DateUtils}


trait LiveEventWrapBase extends Serializable {

}

case class LiveEventWrap (
    val eventTime : Long,
    val partnerId : Int,
    val entryId : String,
    val country : String,
    val city : String,
    val referrer : String,
    val plays : Long,
    val alive : Long,
    val dvrAlive : Long,
    val bitrate : Long,
    val bitrateCount : Long,
    val bufferTime : Long,
    val ipAddress : String
) extends LiveEventWrapBase

//object LiveEvent
//{
//     def parse( line: String ) : LiveEvent =
//     {
//          new LiveEvent()
//     }
//}

class LiveEvent (
     var eventTime : Long,
     var eventRoundTime: Long,
     var partnerId : Int,
     var entryId : String,
     var country : String,
     var city : String,
     var referrer : String,
     var plays : Long,
     var alive : Long,
     var dvrAlive : Long,
     var bitrate : Long,
     var bitrateCount : Long,
     var bufferTime : Long,
     var ipAddress : String
) extends Serializable with MetaLog[BaseLog]
{
     val apacheLogRegex: Pattern = Pattern.compile("^([\\d.]+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) \"([^\"]+)\".*")

     def isNull() : Boolean =
     {
          eventTime == 0
     }

     def roundTimeToHour() =
     {
          this.eventTime = DateUtils.roundTimeToHour(this.eventTime)
          this
     }

     def this()
     {
          this(0, 0, 0, "N/A", "N/A", "N/A", "N/A", 0,0,0,0,0,0, "")
     }

     def +(that: LiveEvent): LiveEvent =
          new LiveEvent(this.eventTime, this.eventRoundTime, this.partnerId, this.entryId, this.country, this.city, this.referrer, this.plays + that.plays, this.alive + that.alive,
               this.dvrAlive + that.dvrAlive, this.bitrate + that.bitrate, this.bitrateCount + that.bitrateCount, this.bufferTime + that.bufferTime, this.ipAddress)

     def max(that: LiveEvent): LiveEvent =
          new LiveEvent(this.eventTime, this.eventRoundTime, this.partnerId, this.entryId, this.country, this.city, this.referrer, math.max(this.plays,that.plays), math.max(this.alive,that.alive),
               math.max(this.dvrAlive,that.dvrAlive), math.max(this.bitrate,that.bitrate), math.max(this.bitrateCount,that.bitrateCount),
               math.max(this.bufferTime,that.bufferTime), this.ipAddress)

     def maxTime(that: LiveEvent): LiveEvent =
          new LiveEvent(math.max(this.eventTime, that.eventTime), math.max(this.eventRoundTime, that.eventRoundTime), this.partnerId, this.entryId, this.country, this.city, this.referrer, this.plays, this.alive,
               this.dvrAlive, this.bitrate, this.bitrateCount, this.bufferTime, this.ipAddress)

     def wrap(withRoundTime: Boolean = false) : LiveEventWrap =
     {
          LiveEventWrap(
          eventTime = if (withRoundTime) eventRoundTime else eventTime,
          partnerId,
          entryId,
          country,
          city,
          referrer,
          plays,
          alive,
          dvrAlive,
          bitrate,
          bitrateCount,
          bufferTime,
          ipAddress
          )
     }
}

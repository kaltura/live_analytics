package com.kaltura.Live.utils

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

/**
 * Created by didi on 3/9/15.
 */
object DateUtils extends MetaLog[BaseLog]
{
     def metaLog = DateUtils

     val millisecondsPerHour : Long = 1000 * 60 * 60

     private val DATE_FORMAT: String = "dd/MMM/yyyy:HH:mm:ss Z"

     def roundTimeToHour(timestamp: Long) : Long =
     {
          (timestamp / millisecondsPerHour) * millisecondsPerHour
     }

     def roundDate(eventDate: String, centeringOffset: Int): Date =
     {
          val formatDate: SimpleDateFormat = new SimpleDateFormat(DATE_FORMAT)
          try
          {
               val date: Date = formatDate.parse(eventDate)
               return roundDate(date, centeringOffset)
          }
          catch
          {
               case e: ParseException =>
               {
                    logger.error("failed to round date", e)
               }
          }

          return null
     }

     def roundDate(eventDate: Date, centeringOffset: Int): Date =
     {
          val c: Calendar = Calendar.getInstance
          c.setTime(eventDate)
          c.add(Calendar.SECOND, centeringOffset)
          val seconds: Int = c.get(Calendar.SECOND)
          val decSeconds: Int = seconds / 10 * 10
          c.set(Calendar.SECOND, decSeconds)
          c.set(Calendar.MILLISECOND, 0)
          return c.getTime
     }

//     def roundDate(dateLong: Long, centeringOffset: Int): Date =
//     {
//          val date: Date = new Date(dateLong)
//          return roundDate(date, centeringOffset)
//     }

     def roundDate(eventDate: String): Date =
     {
          val formatDate: SimpleDateFormat = new SimpleDateFormat(DATE_FORMAT)
          try
          {
               val date: Date = formatDate.parse(eventDate)
               return roundDate(date, 0)
          }
          catch
          {
               case e: ParseException =>
               {
                    logger.error("failed to round date", e)
               }
          }
          return null
     }
}

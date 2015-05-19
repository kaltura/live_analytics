package com.kaltura.Live.model.parse


import java.io.Serializable
import java.net.URLDecoder
import java.util.regex.Matcher
import java.util.regex.Pattern
import com.kaltura.Live.infra.ConfigurationManager
import com.kaltura.Live.infra.utils.{RestRequestParser}
import com.kaltura.Live.model.{Consts, LiveEvent}
import com.kaltura.Live.utils.{BaseLog, MetaLog, DateUtils}
import com.kaltura.ip2location.{Ip2LocationRecord, SerializableIP2LocationReader}

// TODO - make SerializableIP2LocationReader class thread-safe (currently an instance is created on every call)
object CountryCity extends Serializable with MetaLog[BaseLog]
{

     def parse( ipCode : String ) : CountryCity =
     {
          val reader = new SerializableIP2LocationReader(ConfigurationManager.get("aggr.ip2location_path"))

          try
          {
               val ipRecord: Ip2LocationRecord = reader.getAll(ipCode)

               if ( ipRecord.getCountryLong.length > 35 || ipRecord.getCity.length > 35 )
                    logger.warn("ip: '" + ipCode + "' with Long Country: " + ipRecord.getCountryLong + " Long City: " + ipRecord.getCity)

               new CountryCity(ipRecord.getCountryLong, ipRecord.getCity).repair()
          }
          catch
          {
               case e: Exception =>
               {
                    //println("Failed to parse IP " + ipCode) // maybe add exception description
                    logger.warn("Failed to parse IP '" + ipCode + "' exp: " + e.printStackTrace()) // maybe add exception description
               }

               new CountryCity()
          }
          finally
          {
               reader.close();
          }
     }
}

class CountryCity( var country : String = "N/A", var city : String = "N/A")
{
     def repair(): CountryCity =
     {
          if ( country == "-" )
               country = "N/A"
          if ( city == "-" )
               city = "N/A"

          this
     }

     def output(): Unit =
     {
          println(country + ", " + city)
     }
}

object LiveEventParser extends Serializable with MetaLog[BaseLog]
{
     val apacheLogRegex: Pattern = Pattern.compile("^([\\d.]+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) \"([^\"]+)\".*")

     def parse( line: String ) : LiveEvent =
     {
          val event = new LiveEvent

          val m: Matcher = apacheLogRegex.matcher(line)

          if ( !m.find )
          {
               logger.warn(s"Failed to match pattern event: $line")
               return  event
          }

          val date: String = m.group(2)

          event.eventTime = DateUtils.roundDate(date).getTime

          var query: String = m.group(3)

          try
          {
               query = URLDecoder.decode(query, "UTF-8")
               val querySuffixIndex: Int = query.lastIndexOf(" HTTP/")
               if (querySuffixIndex >= 0) query = query.substring(0, querySuffixIndex)
          }
          catch
          {
               case e: Exception =>
               {
                    logger.warn(s"Failed to decode query string: $query", e)
               }
          }

          if (query.toLowerCase.indexOf("service=livestats") > -1 && query.toLowerCase.indexOf("action=collect") > -1)
          {
               event.ipAddress = m.group(1)

               val countryCity = CountryCity.parse(event.ipAddress)

               event.country = countryCity.country
               event.city = countryCity.city

               val paramsMap = RestRequestParser.splitQuery(query)
               if (paramsMap != null && paramsMap.size > 0)
               {
                    try
                    {
                         if (paramsMap.contains("event:entryId"))
                              event.entryId = paramsMap("event:entryId")

                         if (paramsMap.contains("event:partnerId"))
                              event.partnerId = paramsMap("event:partnerId").toInt

                         if (paramsMap.contains("event:bufferTime"))
                              event.bufferTime = (paramsMap("event:bufferTime").toDouble * Consts.BufferTimeResolution).toLong

                         if (paramsMap.contains("event:bitrate"))
                              event.bitrate = paramsMap("event:bitrate").toLong

                         if (paramsMap.contains("event:referrer"))
                              event.referrer = paramsMap("event:referrer")

                         event.bitrateCount = 1
                         if (event.bitrate < 0)
                         {
                              event.bitrate = 0
                              event.bitrateCount = 0
                         }
                         var eventIndex = 0
                         if (paramsMap.contains("event:eventIndex"))
                              eventIndex = paramsMap("event:eventIndex").toInt

                         var eventTypeInt = 1
                         if (paramsMap.contains("event:eventType"))
                              eventTypeInt = paramsMap("event:eventType").toInt

//                         var eventType = LiveEventType.LIVE_EVENT
//                         if ((eventTypeInt != 1))
//                              eventType = LiveEventType.DVR_EVENT

                         if (eventTypeInt == 1)
                         {
                              event.plays = if (eventIndex == 1) 1 else 0
                              event.alive = if (eventIndex > 1) 1 else 0
                         }
                         else
                         {
                              event.dvrAlive = if (eventIndex > 1) 1 else 0
                         }
                         var seconds: Int = 5
                         if (paramsMap.contains("event:startTime"))
                         {
                              val clientEventTime: String = paramsMap("event:startTime")
                              if (clientEventTime.length > 10)
                              {
                                   if (clientEventTime.lastIndexOf(':') >= 0)
                                   {
                                        val secondsString: String = clientEventTime.substring(clientEventTime.lastIndexOf(':') + 1, clientEventTime.lastIndexOf(' '))
                                        seconds = secondsString.toInt
                                   }
                              }
                         }
                         val secondsLastDigit: Int = seconds % 10
                         val offset: Int = 5 - secondsLastDigit
                         event.eventTime = DateUtils.roundDate(date, offset).getTime
                    }
                    catch
                    {
                         case ex: NumberFormatException =>
                         {
                              logger.error("Failed to parse line " + line)
                         }
                    }
               }
          }

          event
     }

}

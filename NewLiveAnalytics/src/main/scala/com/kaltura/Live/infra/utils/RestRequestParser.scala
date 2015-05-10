package com.kaltura.Live.infra.utils

import com.kaltura.Live.utils.{BaseLog, MetaLog}

import java.io.UnsupportedEncodingException
import java.net.URLDecoder

import scala.collection.mutable


object RestRequestParser extends MetaLog[BaseLog]
{
     def metaLog = RestRequestParser

     def splitQuery(query: String): mutable.HashMap[String, String] =
     {
          var queryPairs = scala.collection.mutable.HashMap.empty[String,String]//Map[String, String] = null

          val pairs: Array[String] = query.split("&")

          for (pair <- pairs)
          {
               val idx: Int = pair.indexOf("=")
               try
               {
                    val key = URLDecoder.decode(pair.substring(0, idx), "UTF-8")
                    val value = URLDecoder.decode(pair.substring(idx + 1), "UTF-8")
                    queryPairs += ( URLDecoder.decode(pair.substring(0, idx), "UTF-8") -> URLDecoder.decode(pair.substring(idx + 1), "UTF-8") )
               }
               catch
               {
                    case e: UnsupportedEncodingException =>
                    {
                         logger.error("RestRequestParser: failed to parse request: " + query, e)
                    }
                    case ex: Exception =>
                    {
                         logger.error("RestRequestParser: Failed to parse request: " + query, ex)
                    }
               }
          }

          return queryPairs
     }
}


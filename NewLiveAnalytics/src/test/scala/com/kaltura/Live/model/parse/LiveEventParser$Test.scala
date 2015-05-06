package com.kaltura.Live.model.parse

import org.scalatest.FunSuite

import scala.io.Source

/**
 * Created by didi on 5/4/15.
 */
class LiveEventParser$Test
extends FunSuite
{
     val ipsListFile = "/home/didi/Workspace/Data/ips.txt"

     test("empty ip string")
     {
          val countryCity = CountryCity.parse("")
          assert(countryCity.country==="N/A")
          assert(countryCity.city==="N/A")
     }

     test("list of ips")
     {
          // processing remaining lines
          for( ipString <- Source.fromFile(ipsListFile).getLines() )
          {
               val ipCode = ipString.trim()
               val countryCity = CountryCity.parse(ipCode)
               println(countryCity.country + ", " + countryCity.city)
               assert(countryCity.country!="-")
               assert(countryCity.city!="-")
               // split line by comma and process them
               //l.split(",").map { c =>
                    // your logic here
               //}
          }
     }

}

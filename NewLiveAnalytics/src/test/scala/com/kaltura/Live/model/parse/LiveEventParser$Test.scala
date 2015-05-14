package com.kaltura.Live.model.parse

import com.kaltura.Live.infra.ConfigurationManager
import com.kaltura.ip2location.SerializableIP2LocationReader
import org.scalatest.FunSuite

import scala.io.Source

/**
 * Created by didi on 5/4/15.
 */
class LiveEventParser$Test
extends FunSuite
{
     val ipsListFile = "/home/didi/Workspace/Data/ips.txt"

//     test("empty ip string")
//     {
//          val reader = new SerializableIP2LocationReader(ConfigurationManager.get("aggr.ip2location_path"))
//          val countryCity = CountryCity.parse("", ,reader)
//          assert(countryCity.country==="N/A")
//          assert(countryCity.city==="N/A")
//     reader.close()
//     }

     test("certain problematic ips")
     {
          val reader = new SerializableIP2LocationReader(ConfigurationManager.get("aggr.ip2location_path"))

          var countryCity = CountryCity.parse("187.207.36.1",reader)
          println(countryCity.country + ", " + countryCity.city)

          countryCity = CountryCity.parse("189.243.126.161",reader)
          println(countryCity.country + ", " + countryCity.city)

          countryCity = CountryCity.parse("189.217.142.96",reader)
          println(countryCity.country + ", " + countryCity.city,reader)

          reader.close()
     }

//     test("list of ips")
//     {
//val reader = new SerializableIP2LocationReader(ConfigurationManager.get("aggr.ip2location_path"))
//          // processing remaining lines
//          for( ipString <- Source.fromFile(ipsListFile).getLines() )
//          {
//               val ipCode = ipString.trim()
//               val countryCity = CountryCity.parse(ipCode, reader)
//               println(countryCity.country + ", " + countryCity.city)
//               assert(countryCity.country!="-")
//               assert(countryCity.city!="-")
//               // split line by comma and process them
//               //l.split(",").map { c =>
//                    // your logic here
//               //}
//          }
//     reader.close()
//     }

}

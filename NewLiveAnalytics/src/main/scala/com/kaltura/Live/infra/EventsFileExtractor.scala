package com.kaltura.Live.infra

import com.datastax.driver.core.Cluster
import com.kaltura.Live.env.EnvParams
import com.kaltura.Live.model.dao.LoggedDataCF

import scala.concurrent.Await


class EventsFileExtractor extends Serializable
{
     def fileIdToLines( fileId: String ) : List[String] =
     {
          //val rdd: SchemaRDD = cc.sql( "SELECT data from kaltura_live.log_data WHERE file_id='" + fileId + "'" )
          // val lines = rdd.flatMap(row => blobToLines(row(0) ) ).collect().toList

          val cluster = Cluster.builder().addContactPoint(EnvParams.cassandraAddress).build()
          val session = cluster.connect(EnvParams.kalturaKeySpace)

          val loggedDataCF = new LoggedDataCF(session)

          val blob = Await.result(loggedDataCF.selectFile(fileId), scala.concurrent.duration.Duration.Inf).data

          val lines = BlobExtractor.blobToLines(blob)

          lines

//          val lines = List("1")
//          lines
     }
}

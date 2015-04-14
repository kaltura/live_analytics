package com.kaltura.Live.infra



import com.datastax.driver.core.Cluster
import com.kaltura.Live.env.EnvParams
import com.kaltura.Live.model.dao.{LoggedFile, BatchIdCF, LoggedDataCF, LoggedFilesCF}
import com.kaltura.Live.model.parse.LiveEventParser
import com.kaltura.Live.utils.{BaseLog, MetaLog}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer


//import org.apache.spark.sql.SchemaRDD

//import org.apache.spark.sql.cassandra.CassandraSQLContext

import scala.collection.immutable.List

import com.kaltura.Live.model.LiveEvent

import scala.concurrent.Await

object EventsGenerator extends MetaLog[BaseLog]
{
     def metaLog = EventsGenerator
}

class EventsGenerator( val sc : SparkContext, val maxProcessFilesPerCycle : Int ) extends Serializable with MetaLog[BaseLog]
{


     //final def logger = MustBeLightweight.logger
     //val cc = new CassandraSQLContext(sc)

     val cluster = Cluster.builder().addContactPoint(EnvParams.cassandraAddress).build()
     val session = cluster.connect(EnvParams.kalturaKeySpace)

     var batchId : Long = 0

     var needCheckRecovery: Boolean = true

     val NONE_BATCH_ID = -1

     var needCommit_ : Boolean = false

     getLastBatchId()

     def getLastBatchId()
     {
          //val rdd: SchemaRDD = cc.sql("SELECT * from kaltura_live.log_files WHERE batch_id=-1")

          val batchIdCF = new BatchIdCF(session)
          batchId = Await.result(batchIdCF.getBatchIdIfFound, scala.concurrent.duration.Duration.Inf).batch_id
     }

     def getNonProcessedLoggedFiles() : List[LoggedFile] =
     {
          val loggedFilesCF = new LoggedFilesCF(session)

          var nextLoggedFilesList: List[LoggedFile] = Nil

          if (needCheckRecovery)
          {
               nextLoggedFilesList = Await.result(loggedFilesCF.selectLoggedFiles(batchId), scala.concurrent.duration.Duration.Inf).toList
               needCheckRecovery = false
          }

          if ( nextLoggedFilesList.isEmpty )
               nextLoggedFilesList = Await.result(loggedFilesCF.selectLoggedFiles(NONE_BATCH_ID), scala.concurrent.duration.Duration.Inf).toList

          nextLoggedFilesList
     }

     def get() : RDD[LiveEvent] =
     {
          //val rdd: SchemaRDD = cc.sql( "SELECT file_id from kaltura_live.log_files WHERE batch_id=-1 ORDER BY insert_time LIMIT " + maxProcessFilesPerCycle.toString )
          //rdd.flatMap(row => fileIdToLines(row.getString(0) ) ).map(line => LiveEvent.parse(line) )

          val nonProcessedLoggedFilesList = getNonProcessedLoggedFiles()

          val nextBatchLoggedFiles = sc.parallelize(nonProcessedLoggedFilesList)
               //.map(loggedFile => (loggedFile.insert_time.getTime, loggedFile.file_id) )
               .takeOrdered(maxProcessFilesPerCycle)(Ordering[Long].on(x => x.insert_time.getTime) )
               .toList
               //.sortBy(x => x._1, false)

          val nProcessedFiles = nextBatchLoggedFiles.size

          logger.info(s"number of processed files: $nProcessedFiles")

          if ( nextBatchLoggedFiles.isEmpty )
               return sc.emptyRDD

          preCommit(nextBatchLoggedFiles)

          val nextBatchFileIds = nextBatchLoggedFiles.map(x => x.file_id)

          //val eventsFileExtractor = new EventsFileExtractor(/*session*/)

          //val dummyFilesIds = Array("pa-live-stats1-20150330133932")

          val events = sc.parallelize(nextBatchFileIds)
               .flatMap(fileId => EventsFileExtractor.fileIdToLines(fileId) )
               .map(line => LiveEventParser.parse(line) )
               .filter( !_.isNull )

          val nEvents = events.count()

          logger.info(s"number of processed events: $nEvents")

          events
     }

     def preCommit( loggedFilesList: List[LoggedFile] )
     {
          assert(!loggedFilesList.isEmpty)

          if ( loggedFilesList.isEmpty )
               return

          needCommit_ = true

          val loggedFilesCF = new LoggedFilesCF(session)

          loggedFilesList.map(_.setBatchId(batchId) )
               .map(loggedFilesCF.update(_) )
     }

     def commit()
     {
          if ( !needCommit_ )
               return

          needCommit_ = false
          batchId += 1
          val batchIdCF = new BatchIdCF(session)
          batchIdCF.updateBatchId(batchId)
     }

     def close: Unit =
     {
          session.close()
          cluster.close()
     }

     //-----------------------------------------------------------------------------------------------
     // For Testing
     def getSIM() : RDD[LiveEvent] =
     {
          val timestamp: Long = System.currentTimeMillis

          sc.parallelize(List(new LiveEvent(timestamp, 1, "ent1", "ISR", "TLV", "ref1", 1, 1, 0, 1, 0, 1,"1.1.1.1") ) )
     }

     def commitSIM()
     {

     }
}

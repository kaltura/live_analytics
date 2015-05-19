package com.kaltura.Live.model.purge

import java.util.Date

import com.kaltura.Live.infra.ConfigurationManager
import com.kaltura.Live.model.Consts
import com.kaltura.Live.utils.{BaseLog, MetaLog}
import org.apache.spark.SparkContext
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.toSparkContextFunctions


/**
 * Created by ofirk on 5/6/15.
 */
class DataCleaner(sc: SparkContext) extends Serializable with MetaLog[BaseLog] {

  var currentIteration = 0
  var runOnIteration = ConfigurationManager.get("data_cleaner.num_of_iterations_to_run","20").toInt

  def tryRun() {
    currentIteration += 1
    logger.info(s"Current data cleaner iteration is $currentIteration")
    if (currentIteration == runOnIteration) {
      run()
      currentIteration = 0
    }
  }

  def run() {
    logger.info(s"Cleaning data (this is good)!")
    val tStart = System.nanoTime()
    removeProcessedLogFiles()
    removeLiveEvents()
    val tEnd = System.nanoTime()
    logger.info(s"Done cleaning data! took ${tEnd - tStart}ns")
  }

  def runOn(runOnIteration: Int) {
    this.runOnIteration = runOnIteration
  }

  // TODO - refactor this code to reduce code duplication
  def removeProcessedLogFiles() {

    val dateBefore2Days = new Date(new Date().getTime() - 1 * 24 * 3600 * 1000L );
    val processedFilesRows = sc.cassandraTable(Consts.KalturaKeySpace, "log_files")
      .where("insert_time < ?", dateBefore2Days)
      .select("file_id")
    val filesCount = processedFilesRows.count()
    logger.info(s"Trying to delete $filesCount files")
    val connector = CassandraConnector(sc.getConf)
    processedFilesRows.foreachPartition(partition => {
      connector.withSessionDo {
        session => {
          partition.foreach { row =>
            session.execute(s"DELETE FROM ${Consts.KalturaKeySpace}.log_data where file_id='${row.getString(0)}';")
            session.execute(s"DELETE FROM ${Consts.KalturaKeySpace}.log_files where file_id='${row.getString(0)}';")
          }
        }
      }
    })
    logger.info(s"Deleted $filesCount files!")
  }

  def removeLiveEvents() {

    val dateBefore2Days = new Date(new Date().getTime() - 36 * 3600 * 1000L );
    val liveEventRows = sc.cassandraTable(Consts.KalturaKeySpace, "live_events")
      .where("event_time < ?", dateBefore2Days)
      .limit(ConfigurationManager.get("data_cleaner.max_num_of_rows_to_delete","1000").toLong)
      .select("entry_id","event_time")
      .as((_: String, _: Long))
    val liveEventsCount = liveEventRows.count()
    logger.info(s"Trying to delete $liveEventsCount live_events rows")
    val connector = CassandraConnector(sc.getConf)
    liveEventRows.foreachPartition(partition => {
      connector.withSessionDo {
        session => {
          partition.foreach { row =>
            session.execute(s"DELETE FROM ${Consts.KalturaKeySpace}.live_events where entry_id='${row._1}' and event_time='${row._2}';")
            session.execute(s"DELETE FROM ${Consts.KalturaKeySpace}.live_events_location where entry_id='${row._1}' and event_time='${row._2}';")
          }
        }
      }
    })
    logger.info(s"Deleted $liveEventsCount live_events rows")
  }

}

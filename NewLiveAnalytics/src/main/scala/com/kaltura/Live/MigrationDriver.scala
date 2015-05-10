package com.kaltura.Live

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.RowWriterFactory
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import com.kaltura.Live.infra.ConfigurationManager
import com.kaltura.Live.migration.SchemaMigrationCQL
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.Logger
import scala.util.Try

import scala.reflect.ClassTag

/**
 * Created by ofirk on 4/12/15.
 */
object MigrationDriver {

  val log = Logger.getLogger(getClass.getName)

  val liveKeyspace = "kaltura_live"
  var tableNameSuffix = ""

  case class LiveEvent               (entry_id: String, event_time: Long, alive: Long, dvr_alive: Long, bitrate: Long, bitrate_count: Long, buffer_time: Long, plays: Long)
  case class HourlyLiveEvent         (entry_id: String, event_time: Long, alive: Long, dvr_alive: Long, bitrate: Long, bitrate_count: Long, buffer_time: Long, plays: Long)
  case class LiveEventLocation       (entry_id: String, event_time: Long, country:String, city:String, alive: Long, dvr_alive: Long, bitrate: Long, bitrate_count: Long, buffer_time: Long, plays: Long)
  case class HourlyLiveEventReferrer (entry_id: String, event_time: Long, referrer: String, alive: Long, dvr_alive: Long, bitrate: Long, bitrate_count: Long, buffer_time: Long, plays: Long)
  case class HourlyLiveEventPartner  (partner_id: String, event_time: Long, alive: Long, dvr_alive: Long, bitrate: Long, bitrate_count: Long, buffer_time: Long, plays: Long)


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster(ConfigurationManager.get("spark.master"))
      .setAppName("LiveAnalyticsSchemaMigration")
      .set("spark.executor.memory", ConfigurationManager.get("spark.executor_memory", "1g"))
      .set("spark.cassandra.connection.host", ConfigurationManager.get("cassandra.node_name"))
    val dryRun = ConfigurationManager.get("migration.dry_run", "false").toBoolean

    val sc = new SparkContext(conf)

    CassandraConnector(conf).withSessionDo { session =>
      SchemaMigrationCQL.BackupTablesCreationScript.foreach { command =>
        session.execute(command)
      }
    }

    copyTableContent[LiveEvent](sc, "live_events", "live_events_backup")
    copyTableContent[HourlyLiveEvent](sc, "hourly_live_events", "hourly_live_events_backup")
    copyTableContent[LiveEventLocation](sc, "live_events_location", "live_events_location_backup")
    copyTableContent[HourlyLiveEventPartner](sc, "hourly_live_events_partner", "hourly_live_events_partner_backup")
    copyTableContent[HourlyLiveEventReferrer](sc, "hourly_live_events_referrer", "hourly_live_events_referrer_backup")

    CassandraConnector(conf).withSessionDo { session =>
      if (dryRun) {
        SchemaMigrationCQL.TestTablesCreationScript.foreach { command =>
          session.execute(command)
        }
      }
      else {
        SchemaMigrationCQL.TablesDropScript.foreach { command =>
          session.execute(command)
        }
        SchemaMigrationCQL.TablesCreationScript.foreach { command =>
          session.execute(command)
        }
      }
    }

    if (dryRun) {
      tableNameSuffix = "_test"
    }

    copyTableContent[LiveEvent](sc, "live_events_backup", "live_events" + tableNameSuffix, 100)
    copyTableContent[HourlyLiveEvent](sc, "hourly_live_events_backup", "hourly_live_events" + tableNameSuffix, 100)
    copyTableContent[LiveEventLocation](sc, "live_events_location_backup", "live_events_location" + tableNameSuffix, 100)
    copyTableContent[HourlyLiveEventPartner](sc, "hourly_live_events_partner_backup", "hourly_live_events_partner" + tableNameSuffix, 100)
    copyTableContent[HourlyLiveEventReferrer](sc, "hourly_live_events_referrer_backup", "hourly_live_events_referrer" + tableNameSuffix, 100)

  }

  def copyTableContent[T](sc: SparkContext, sourceTableName: String, destTableName: String, multiplyBufferTime: Int = 1)
                    (implicit connector: CassandraConnector = CassandraConnector(sc.getConf),
                              ct: ClassTag[T], rrf: RowReaderFactory[T],
                              ev: ValidRDDType[T],
                              rwf: RowWriterFactory[T]) = {
    val entryTable = sc.cassandraTable[T](liveKeyspace, sourceTableName)
    log.info("Copying data from " + sourceTableName + " to " + destTableName + "...")
    entryTable
      .map(
        row => {
          copyRow[T](row, multiplyBufferTime)
        }
      )
      .saveToCassandra(liveKeyspace, destTableName, entryTable.columnNames)
    log.info("Done copying data from " + sourceTableName + " to " + destTableName + "...")
  }

  def copyRow[T](row: T,  multiplyBufferTime: Int): T = row match {
    case row: LiveEvent => row.copy(buffer_time = row.buffer_time * multiplyBufferTime).asInstanceOf[T]
    case row: HourlyLiveEvent => row.copy(buffer_time = row.buffer_time * multiplyBufferTime).asInstanceOf[T]
    case row: LiveEventLocation => row.copy(buffer_time = row.buffer_time * multiplyBufferTime).asInstanceOf[T]
    case row: HourlyLiveEventPartner => row.copy(buffer_time = row.buffer_time * multiplyBufferTime).asInstanceOf[T]
    case row: HourlyLiveEventReferrer => row.copy(buffer_time = row.buffer_time * multiplyBufferTime).asInstanceOf[T]
    case _ => row
  }
}

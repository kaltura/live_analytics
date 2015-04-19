package com.kaltura.Live.migration

/**
 * Created by ofirk on 4/16/15.
 */
object SchemaMigrationCQL {
  val BackupTablesCreationScript = Array(
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.live_events_backup (
      	entry_id text,
      	event_time timestamp,
      	plays bigint,
      	alive bigint,
      	dvr_alive bigint,
      	bitrate bigint,
      	bitrate_count bigint,
      	buffer_time double,
      	PRIMARY KEY (entry_id,event_time)
      )
      WITH CLUSTERING ORDER BY (event_time DESC);
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.hourly_live_events_backup (
      	entry_id text,
      	event_time timestamp,
      	plays bigint,
      	alive bigint,
      	dvr_alive bigint,
      	bitrate bigint,
      	bitrate_count bigint,
      	buffer_time double,
      	PRIMARY KEY (entry_id,event_time)
      )
      WITH CLUSTERING ORDER BY (event_time DESC);
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.live_events_location_backup (
      	entry_id text,
      	event_time timestamp,
      	country text,
      	city text,
      	plays bigint,
      	alive bigint,
      	dvr_alive bigint,
      	bitrate bigint,
      	bitrate_count bigint,
      	buffer_time double,
      	PRIMARY KEY ((entry_id,event_time), country, city));
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.hourly_live_events_partner_backup (
      	partner_id int,
      	event_time timestamp,
      	plays bigint,
      	alive bigint,
      	dvr_alive bigint,
      	bitrate bigint,
      	bitrate_count bigint,
      	buffer_time double,
      	PRIMARY KEY (partner_id,event_time)
      )
      WITH CLUSTERING ORDER BY (event_time DESC);
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.hourly_live_events_referrer_backup (
      	entry_id text,
      	event_time timestamp,
      	referrer text,
      	plays bigint,
      	alive bigint,
      	dvr_alive bigint,
      	bitrate bigint,
      	bitrate_count bigint,
      	buffer_time double,
      	PRIMARY KEY ((entry_id,event_time), referrer));
    """)
  val TestTablesCreationScript = Array(
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.live_events_test (
        entry_id text,
        event_time timestamp,
        plays counter,
        alive counter,
        dvr_alive counter,
        bitrate counter,
        bitrate_count counter,
        buffer_time counter,
        PRIMARY KEY (entry_id,event_time)
      )
      WITH CLUSTERING ORDER BY (event_time DESC);
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.hourly_live_events_test (
        entry_id text,
        event_time timestamp,
        plays counter,
        alive counter,
        dvr_alive counter,
        bitrate counter,
        bitrate_count counter,
        buffer_time counter,
        PRIMARY KEY (entry_id,event_time)
      )
      WITH CLUSTERING ORDER BY (event_time DESC);
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.live_events_location_test (
        entry_id text,
        event_time timestamp,
        country text,
        city text,
        plays counter,
        alive counter,
        dvr_alive counter,
        bitrate counter,
        bitrate_count counter,
        buffer_time counter,
        PRIMARY KEY ((entry_id,event_time), country, city));
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.hourly_live_events_partner_test (
        partner_id int,
        event_time timestamp,
        plays counter,
        alive counter,
        dvr_alive counter,
        bitrate counter,
        bitrate_count counter,
        buffer_time counter,
        PRIMARY KEY (partner_id,event_time)
      )
      WITH CLUSTERING ORDER BY (event_time DESC);
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.hourly_live_events_referrer_test (
        entry_id text,
        event_time timestamp,
        referrer text,
        plays counter,
        alive counter,
        dvr_alive counter,
        bitrate counter,
        bitrate_count counter,
        buffer_time counter,
        PRIMARY KEY ((entry_id,event_time), referrer));
    """)
  val TablesDropScript = Array(
    "DROP TABLE kaltura_live.live_events;",
    "DROP TABLE kaltura_live.hourly_live_events;",
    "DROP TABLE kaltura_live.live_events_location;",
    "DROP TABLE kaltura_live.hourly_live_events_partner;",
    "DROP TABLE kaltura_live.hourly_live_events_referrer;"
  )
  val TablesCreationScript = Array(
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.live_events (
        entry_id text,
        event_time timestamp,
        plays counter,
        alive counter,
        dvr_alive counter,
        bitrate counter,
        bitrate_count counter,
        buffer_time counter,
        PRIMARY KEY (entry_id,event_time)
      )
      WITH CLUSTERING ORDER BY (event_time DESC);
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.hourly_live_events (
        entry_id text,
        event_time timestamp,
        plays counter,
        alive counter,
        dvr_alive counter,
        bitrate counter,
        bitrate_count counter,
        buffer_time counter,
        PRIMARY KEY (entry_id,event_time)
      )
      WITH CLUSTERING ORDER BY (event_time DESC);
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.live_events_location (
        entry_id text,
        event_time timestamp,
        country text,
        city text,
        plays counter,
        alive counter,
        dvr_alive counter,
        bitrate counter,
        bitrate_count counter,
        buffer_time counter,
        PRIMARY KEY ((entry_id,event_time), country, city));
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.hourly_live_events_partner (
        partner_id int,
        event_time timestamp,
        plays counter,
        alive counter,
        dvr_alive counter,
        bitrate counter,
        bitrate_count counter,
        buffer_time counter,
        PRIMARY KEY (partner_id,event_time)
      )
      WITH CLUSTERING ORDER BY (event_time DESC);
    """,
    """
      CREATE TABLE IF NOT EXISTS kaltura_live.hourly_live_events_referrer (
        entry_id text,
        event_time timestamp,
        referrer text,
        plays counter,
        alive counter,
        dvr_alive counter,
        bitrate counter,
        bitrate_count counter,
        buffer_time counter,
        PRIMARY KEY ((entry_id,event_time), referrer));
    """)
}

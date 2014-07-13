CREATE TABLE hourly_live_events (
  entry_id text,
  event_time timestamp,
  alive bigint,
  bitrate bigint,
  bitrate_count bigint,
  buffer_time bigint,
  plays bigint,
  PRIMARY KEY (entry_id, event_time)
);

INSERT INTO hourly_live_events (entry_id, event_time, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 11:00:00-0500', 5, 10, 5, 20, 3);
INSERT INTO hourly_live_events (entry_id, event_time, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 10:00:00-0500', 10, 100, 3, 15, 4);

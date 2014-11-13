CREATE TABLE live_events (
  entry_id text,
  event_time timestamp,
  alive bigint,
  bitrate bigint,
  bitrate_count bigint,
  buffer_time bigint,
  plays bigint,
  PRIMARY KEY (entry_id, event_time)
);

CREATE TABLE live_entry_hourly_peak (
  entry_id text,
  event_time timestamp,
  audience bigint,
  PRIMARY KEY ((entry_id), event_time)
);

INSERT INTO live_events (entry_id, event_time, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 10:38:20-0500', 5, 10, 5, 20, 3);
INSERT INTO live_events (entry_id, event_time, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 10:37:20-0500', 10, 100, 3, 15, 3);
INSERT INTO live_events (entry_id, event_time, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 10:36:20-0500', 15, 90, 2, 5, 4);
INSERT INTO live_events (entry_id, event_time, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 11:30:00-0500', 15, 90, 2, 5, 4);

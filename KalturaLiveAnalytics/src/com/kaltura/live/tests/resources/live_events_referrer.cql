CREATE TABLE hourly_live_events_referrer (
  entry_id text,
  event_time timestamp,
  referrer text,
  alive bigint,
  bitrate bigint,
  bitrate_count bigint,
  buffer_time bigint,
  plays bigint,
  PRIMARY KEY ((entry_id, event_time), referrer)
);

INSERT INTO hourly_live_events_referrer (entry_id, event_time, referrer, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 10:00:00-0500', 'testA', 5, 10, 5, 20, 3);
INSERT INTO hourly_live_events_referrer (entry_id, event_time, referrer, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 10:00:00-0500', 'testB', 10, 100, 3, 15, 50);
INSERT INTO hourly_live_events_referrer (entry_id, event_time, referrer, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 10:00:00-0500', 'testC', 15, 90, 2, 5, 100);
INSERT INTO hourly_live_events_referrer (entry_id, event_time, referrer, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 11:00:00-0500', 'testA', 5, 10, 5, 20, 3);
INSERT INTO hourly_live_events_referrer (entry_id, event_time, referrer, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 11:00:00-0500', 'testB', 10, 100, 3, 15, 50);
INSERT INTO hourly_live_events_referrer (entry_id, event_time, referrer, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 11:00:00-0500', 'testC', 15, 90, 2, 5, 100);


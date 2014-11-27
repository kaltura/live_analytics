CREATE TABLE live_events_location (
  entry_id text,
  event_time timestamp,
  country text,
  city text,
  alive bigint,
  bitrate bigint,
  bitrate_count bigint,
  buffer_time bigint,
  plays bigint,
  PRIMARY KEY ((entry_id, event_time), country, city)
) ;

INSERT INTO live_events_location (entry_id, event_time, country, city, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 10:38:20-0500', '-', '-', 5, 10, 5, 20, 3);
INSERT INTO live_events_location (entry_id, event_time, country, city, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 10:38:20-0500', 'AFGHANISTAN', '-', 10, 100, 3, 15, 3);
INSERT INTO live_events_location (entry_id, event_time, country, city, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 10:38:20-0500', 'AFGHANISTAN','CHARIKAR', 15, 90, 2, 5, 4);
INSERT INTO live_events_location (entry_id, event_time, country, city, alive, bitrate, bitrate_count, buffer_time, plays) values('test_entry','2013-12-15 10:38:30-0500', '-', '-', 5, 10, 5, 20, 3);
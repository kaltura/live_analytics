CREATE TABLE hourly_live_events_partner (
  partner_id int,
  event_time timestamp,
  alive bigint,
  bitrate bigint,
  bitrate_count bigint,
  buffer_time bigint,
  plays bigint,
  PRIMARY KEY (partner_id, event_time)
);

INSERT INTO hourly_live_events_partner (partner_id, event_time, alive, bitrate, bitrate_count, buffer_time, plays) values(777,'2013-12-15 11:00:00-0500', 5, 10, 5, 20, 3);
INSERT INTO hourly_live_events_partner (partner_id, event_time, alive, bitrate, bitrate_count, buffer_time, plays) values(777,'2013-12-15 10:00:00-0500', 10, 100, 3, 15, 4);

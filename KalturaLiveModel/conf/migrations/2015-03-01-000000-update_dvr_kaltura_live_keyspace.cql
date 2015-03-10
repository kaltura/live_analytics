use kaltura_live;

ALTER TABLE live_events ADD dvr_alive bigint;
ALTER TABLE hourly_live_events ADD dvr_alive bigint;
ALTER TABLE live_events_location ADD dvr_alive bigint;
ALTER TABLE hourly_live_events_partner ADD dvr_alive bigint;
ALTER TABLE hourly_live_events_referrer ADD dvr_alive bigint;
ALTER TABLE live_entry_hourly_peak ADD dvr_audience bigint;



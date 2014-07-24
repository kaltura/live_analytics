CREATE TABLE live_partner_entry (
  partner_id int,
  entry_id text,
  event_time timestamp,
  PRIMARY KEY (partner_id, entry_id)
);

INSERT INTO live_partner_entry (partner_id, entry_id, event_time) values(777,'test_entry1', '2013-12-15 10:38:20-0500');
INSERT INTO live_partner_entry (partner_id, entry_id, event_time) values(776,'test_entry2', '2013-12-15 10:38:20-0500');
INSERT INTO live_partner_entry (partner_id, entry_id, event_time) values(777,'test_entry3', '2013-12-15 10:38:20-0500');

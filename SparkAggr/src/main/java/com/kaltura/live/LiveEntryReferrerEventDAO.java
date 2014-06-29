package com.kaltura.live;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class LiveEntryReferrerEventDAO extends LiveEventDAO {
	
	PreparedStatement statement;
	
	public LiveEntryReferrerEventDAO(SerializableSession session) {
		super(session);

		// TODO Auto-generated constructor stub
	}

	public void init(StatsEvent event)
	{
		super.init(event);
		if (statement == null) {
			statement = session.getSession().prepare("INSERT INTO kaltura_live.live_events_referrer (entry_id, event_time, referrer, plays, alive, bitrate, bitrate_count, buffer_time) " +
					"VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
		}
	}
	
	public void saveOrUpdate() {
		
		BoundStatement boundStatement = new BoundStatement(statement);
		session.getSession().execute(boundStatement.bind(aggrRes.getEntryId(), aggrRes.getEventTime(), aggrRes.getReferrer(), aggrRes.getPlays(), aggrRes.getAlive(), aggrRes.getBitrate(), aggrRes.getBitrateCount(), aggrRes.getBufferTime()));
		
	}
}

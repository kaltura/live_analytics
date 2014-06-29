package com.kaltura.live;

import org.apache.spark.broadcast.Broadcast;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class LiveEntryEventDAO extends LiveEventDAO {
	 
	String tableName;
	PreparedStatement statement;
	
	public LiveEntryEventDAO(SerializableSession session, String tableName) {
		super(session);
		this.tableName = tableName;
		
	}
	
	public void init(StatsEvent event)
	{
		super.init(event);
		if (statement == null) {
			statement = session.getSession().prepare("INSERT INTO " + tableName + " (entry_id, event_time, plays, alive, bitrate, bitrate_count, buffer_time) " +
					"VALUES (?, ?, ?, ?, ?, ?, ?)");
		}
		
	}

	public void saveOrUpdate() {
		BoundStatement boundStatement = new BoundStatement(statement);
		session.getSession().execute(boundStatement.bind(aggrRes.getEntryId(), aggrRes.getEventTime(), aggrRes.getPlays(), aggrRes.getAlive(), aggrRes.getBitrate(), aggrRes.getBitrateCount(), aggrRes.getBufferTime()));
	}
}

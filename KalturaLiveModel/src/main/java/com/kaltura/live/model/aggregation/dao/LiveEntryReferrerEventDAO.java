package com.kaltura.live.model.aggregation.dao;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.StatsEvent;

public class LiveEntryReferrerEventDAO extends LiveEventDAO {
	
	private static final long serialVersionUID = -4766071091288393800L;

	@Override
	public String getTableName() {
		return "kaltura_live.hourly_live_events_referrer";
	}

	@Override
	protected List<String> getTableSpecificFields() {
		List<String> fields = new ArrayList<String>();
		fields.add("referrer");
		return fields;
	}
	
	public void saveOrUpdate(SerializableSession session, StatsEvent aggregatedResult) {
		createStatement(session);
		BoundStatement boundStatement = new BoundStatement(statement);
		session.getSession().execute(boundStatement.bind(aggregatedResult.getEntryId(), aggregatedResult.getEventTime(), aggregatedResult.getReferrer(), aggregatedResult.getPlays(), aggregatedResult.getAlive(), aggregatedResult.getBitrate(), aggregatedResult.getBitrateCount(), aggregatedResult.getBufferTime()));
		
	}
}

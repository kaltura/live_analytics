package com.kaltura.live.model.aggregation.dao;

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.StatsEvent;

public class LiveEntryLocationEventDAO extends LiveEventDAO {
	
	private static final long serialVersionUID = -7242656117403520591L;

	@Override
	public String getTableName() {
		return "kaltura_live.live_events_location";
	}
	
	@Override
	protected List<String> getTableSpecificFields() {
		return java.util.Arrays.asList(new String[]{"country", "city"});
	}
	
	public void saveOrUpdate(SerializableSession session, StatsEvent aggregatedResult) {
		createStatement(session);
		BoundStatement boundStatement = new BoundStatement(statement);
		session.getSession().execute(boundStatement.bind(aggregatedResult.getEntryId(), aggregatedResult.getEventTime(), aggregatedResult.getCountry(), aggregatedResult.getCity(), aggregatedResult.getPlays(), aggregatedResult.getAlive(), aggregatedResult.getBitrate(), aggregatedResult.getBitrateCount(), aggregatedResult.getBufferTime()));
		
	}


}

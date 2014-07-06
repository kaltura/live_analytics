package com.kaltura.live.model.aggregation.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.StatsEvent;

public class PartnerEventDAO extends LiveEventDAO {
	
	private static final long serialVersionUID = 1506062076872874654L;
	
	protected String tableName;
	
	public PartnerEventDAO(String tableName) {
		this.tableName = tableName;
	}
	
	@Override
	protected String getTableName() {
		return tableName;
	}
	
	@Override
	protected List<String> getKeyFields() {
		return Arrays.asList(new String[]{"partner_id", "event_time"});
	}

	@Override
	protected List<String> getTableSpecificFields() {
		return new ArrayList<String>();
	}

	@Override
	public void saveOrUpdate(SerializableSession session, StatsEvent aggregatedResult) {
		createStatement(session);
		BoundStatement boundStatement = new BoundStatement(statement);
		session.getSession().execute(boundStatement.bind(aggregatedResult.getPartnerId(), aggregatedResult.getEventTime(), aggregatedResult.getPlays(), aggregatedResult.getAlive(), aggregatedResult.getBitrate(), aggregatedResult.getBitrateCount(), aggregatedResult.getBufferTime()));
		
	}

}

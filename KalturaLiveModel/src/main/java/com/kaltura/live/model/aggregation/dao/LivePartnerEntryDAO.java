package com.kaltura.live.model.aggregation.dao;

import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.StatsEvent;

public class LivePartnerEntryDAO extends LiveDAO {

	private static final long serialVersionUID = 2179416653993486273L;

	@Override
	protected String getTableName() {
		return "kaltura_live.live_partner_entry";
	}
	
	
	@Override
	public void saveOrUpdate(SerializableSession session, StatsEvent aggregatedResult) {
		createStatement(session);
		BoundStatement boundStatement = new BoundStatement(statement);
		session.getSession().execute(boundStatement.bind(aggregatedResult.getPartnerId(), aggregatedResult.getEntryId(), aggregatedResult.getEventTime()));
		
	}

	@Override
	protected int getTTL() {
		return 37;
	}

	@Override
	protected List<String> getTableFields() {
		return Arrays.asList(new String[]{"partner_id", "entry_id", "event_time"});
	}

}

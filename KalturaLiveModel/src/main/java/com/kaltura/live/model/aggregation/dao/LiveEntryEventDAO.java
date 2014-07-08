package com.kaltura.live.model.aggregation.dao;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.StatsEvent;

/**
 *	Live entry representer 
 */
public class LiveEntryEventDAO extends LiveEventDAO {
	
	private static final long serialVersionUID = 7816082102323233816L;
	
	protected String tableName;

	protected String entryId;
	
	public LiveEntryEventDAO(String tableName) {
		super();
		this.tableName = tableName;
	}
	
	public LiveEntryEventDAO(Row row) {
		super(row);
		this.entryId = row.getString("entry_id");
	}
	
	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	protected List<String> getTableSpecificFields() {
		return new ArrayList<String>();
	}
	
	public void saveOrUpdate(SerializableSession session, StatsEvent aggregatedResult) {
		createStatement(session);
		BoundStatement boundStatement = new BoundStatement(statement);
		session.getSession().execute(boundStatement.bind(aggregatedResult.getEntryId(), aggregatedResult.getEventTime(), aggregatedResult.getPlays(), aggregatedResult.getAlive(), aggregatedResult.getBitrate(), aggregatedResult.getBitrateCount(), aggregatedResult.getBufferTime()));
	}

	public String getEntryId() {
		return entryId;
	}

	public void setEntryId(String entryId) {
		this.entryId = entryId;
	}
}

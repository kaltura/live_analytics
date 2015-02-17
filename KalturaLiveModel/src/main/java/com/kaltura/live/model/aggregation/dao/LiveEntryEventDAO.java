package com.kaltura.live.model.aggregation.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	protected int ttl;

	protected String entryId;
	
	private static Logger LOG = LoggerFactory.getLogger(LiveEntryEventDAO.class);
	
	public LiveEntryEventDAO(String tableName, int ttl) {
		super();
		this.tableName = tableName;
		this.ttl = ttl;
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
		try {
			session.execute(boundStatement.bind(aggregatedResult.getEntryId(), aggregatedResult.getEventTime(), aggregatedResult.getPlays(), aggregatedResult.getAlive(), aggregatedResult.getDVRAlive(), aggregatedResult.getBitrate(), aggregatedResult.getBitrateCount(), aggregatedResult.getBufferTime()), RETRIES_NUM);
		} catch (Exception ex) {
			LOG.error("Failed to save aggregation result for entry [" + aggregatedResult.getEntryId() + "] at [" + aggregatedResult.getEventTime() + "]", ex);
		}
	}

	public String getEntryId() {
		return entryId;
	}

	public void setEntryId(String entryId) {
		this.entryId = entryId;
	}

	@Override
	protected int getTTL() {
		return ttl;
	}
}

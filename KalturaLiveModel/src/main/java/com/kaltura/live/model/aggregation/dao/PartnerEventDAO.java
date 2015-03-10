package com.kaltura.live.model.aggregation.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.StatsEvent;

public class PartnerEventDAO extends LiveEventDAO {
	
	private static final long serialVersionUID = 1506062076872874654L;
	
	private static Logger LOG = LoggerFactory.getLogger(PartnerEventDAO.class); 
	
	protected String tableName;
	protected int ttl;
	
	protected int partnerId;
	
	public PartnerEventDAO(String tableName, int ttl) {
		this.tableName = tableName;
		this.ttl = ttl;
	}
	
	public PartnerEventDAO(Row row) {
		super(row);
		this.partnerId = row.getInt("partner_id");
	}
	
	@Override
	protected String getTableName() {
		return tableName;
	}
	
	@Override
	protected List<String> getKeyFields() {
        return Arrays.<String>asList("partner_id", "event_time");
	}

	@Override
	protected List<String> getTableSpecificFields() {
		return new ArrayList<String>();
	}

	@Override
	public void saveOrUpdate(SerializableSession session, StatsEvent aggregatedResult) {
		createStatement(session);
		BoundStatement boundStatement = new BoundStatement(statement);
		try {
			session.execute(boundStatement.bind(aggregatedResult.getPartnerId(), aggregatedResult.getEventTime(), aggregatedResult.getPlays(), aggregatedResult.getAlive(), aggregatedResult.getDVRAlive(), aggregatedResult.getBitrate(), aggregatedResult.getBitrateCount(), aggregatedResult.getBufferTime()), RETRIES_NUM);
		} catch (Exception ex) {
			LOG.error("Failed to save partner aggregation result for partner [" + aggregatedResult.getPartnerId() +  "] at [" + aggregatedResult.getEventTime() + "]", ex);
		}
		
	}
	
	public int getPartnerId() {
		return partnerId;
	}

	@Override
	protected int getTTL() {
		return ttl;
	}

}

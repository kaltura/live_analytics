package com.kaltura.live.model.aggregation.dao;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.StatsEvent;

public class LiveEntryPeakDAO extends LiveEventDAO {

	private static final long serialVersionUID = 2179416653993486273L;
	
	protected String entryId;
	protected Date eventTime;
	protected Long audience;
	
	private static Logger LOG = LoggerFactory.getLogger(LiveEntryPeakDAO.class); 

		
	public LiveEntryPeakDAO() {
		super();
	}
	
	public LiveEntryPeakDAO(Row row) {
		this.entryId = row.getString("entry_id");
		this.eventTime = row.getDate("event_time");
		this.audience = row.getLong("audience");
		
	}
	
	@Override
	protected String getTableName() {
		return "kaltura_live.live_entry_hourly_peak";
	}
	
	
	@Override
	public void saveOrUpdate(SerializableSession session, StatsEvent aggregatedResult) {
		createStatement(session);
		BoundStatement boundStatement = new BoundStatement(statement);
		try {
		session.execute(boundStatement.bind(aggregatedResult.getEntryId(), aggregatedResult.getEventTime(), aggregatedResult.getAlive() + aggregatedResult.getPlays()), RETRIES_NUM);
		} catch (Exception ex) {
			LOG.error("Failed to save peak aggregation result for entry [" + aggregatedResult.getEntryId() +  "] at [" + aggregatedResult.getEventTime() + "]", ex);
		}
		
	}

	@Override
	protected int getTTL() {
		return 60 * 60 * 37;
	}

	@Override
	protected List<String> getTableFields() {
		return Arrays.asList(new String[]{"entry_id", "event_time", "audience"});
	}

	public String getEntryId() {
		return entryId;
	}

	public void setEntryId(String entryId) {
		this.entryId = entryId;
	}

	public Date getEventTime() {
		return eventTime;
	}

	public void setEventTime(Date eventTime) {
		this.eventTime = eventTime;
	}
	
	public Long getAudience() {
		return audience;
	}
	
	public void setAudience(Long audience) {
		this.audience = audience;
	}

	@Override
	protected List<String> getTableSpecificFields() {
		// TODO Auto-generated method stub
		return null;
	}



}

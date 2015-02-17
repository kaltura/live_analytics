package com.kaltura.live.model.aggregation.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.StatsEvent;

public class LiveEntryReferrerEventDAO extends LiveEventDAO {
	
	private static final long serialVersionUID = -4766071091288393800L;
	
	private static Logger LOG = LoggerFactory.getLogger(LiveEntryReferrerEventDAO.class); 

	protected String referrer;

	protected String entryId;
	
	public LiveEntryReferrerEventDAO() {
		super();
	}
	
	public LiveEntryReferrerEventDAO(Row row) {
		super(row);
		this.entryId = row.getString("entry_id");
		this.referrer = row.getString("referrer");
	}

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
		try {
			session.execute(boundStatement.bind(aggregatedResult.getEntryId(), aggregatedResult.getEventTime(), aggregatedResult.getReferrer(), aggregatedResult.getPlays(), aggregatedResult.getAlive(), aggregatedResult.getDVRAlive(), aggregatedResult.getBitrate(), aggregatedResult.getBitrateCount(), aggregatedResult.getBufferTime()), RETRIES_NUM);
		} catch (Exception ex) {
			LOG.error("Failed to save referrer aggregation result for entry [" + aggregatedResult.getEntryId() + "] referrer [" + aggregatedResult.getReferrer() + "] at [" + aggregatedResult.getEventTime() + "]", ex);
		}
		
	}

	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}

	public String getEntryId() {
		return entryId;
	}

	public void setEntryId(String entryId) {
		this.entryId = entryId;
	}

	@Override
	protected int getTTL() {
		return HOURLY_AGGR_TTL;
	}
	
}

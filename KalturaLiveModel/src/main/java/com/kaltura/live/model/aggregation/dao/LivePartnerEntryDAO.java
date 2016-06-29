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

public class LivePartnerEntryDAO extends LiveDAO {

	private static final long serialVersionUID = 2179416653993486273L;
	
	private static Logger LOG = LoggerFactory.getLogger(LivePartnerEntryDAO.class); 
	
	protected int partnerId;
	public int getPartnerId() {
		return partnerId;
	}
	
	protected String entryId;
	protected Date eventTime;
	
	public LivePartnerEntryDAO() {
		super();
	}
	
	public LivePartnerEntryDAO(Row row) {
		super(row);
		this.partnerId = row.getInt("partner_id");
		this.entryId = row.getString("entry_id");
		this.eventTime = new Date(row.getDate("event_time").getMillisSinceEpoch());
		
	}

	@Override
	protected String getTableName() {
		return "kaltura_live.live_partner_entry";
	}
	
	
	@Override
	public void saveOrUpdate(SerializableSession session, StatsEvent aggregatedResult) {
		createStatement(session);
		BoundStatement boundStatement = new BoundStatement(statement);
		try {
			session.execute(boundStatement.bind(aggregatedResult.getPartnerId(), aggregatedResult.getEntryId(), aggregatedResult.getEventTime()), RETRIES_NUM );
		} catch (Exception ex) {
			LOG.error("Failed to save aggregation result for partner [" + aggregatedResult.getPartnerId() + "] entry [" + aggregatedResult.getEntryId() + "] at [" + aggregatedResult.getEventTime() + "]", ex);
		}
		
	}

	@Override
	protected int getTTL() {
		return 60 * 60 * 37;
	}

	@Override
	protected List<String> getTableFields() {
		return Arrays.asList(new String[]{"partner_id", "entry_id", "event_time"});
	}
	
	public void setPartnerId(int partnerId) {
		this.partnerId = partnerId;
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


}

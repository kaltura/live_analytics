package com.kaltura.live.model.aggregation.dao;

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.StatsEvent;

public class LiveEntryLocationEventDAO extends LiveEventDAO {
	
	private static final long serialVersionUID = -7242656117403520591L;
	
	protected String city;
	protected String country;
	protected String entryId;
	
	public LiveEntryLocationEventDAO() {
		super();
	}
	
	public LiveEntryLocationEventDAO(Row row) {
		super(row);
		this.entryId = row.getString("entry_id");
		this.city = row.getString("city");
		this.country = row.getString("country");
	}

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

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getEntryId() {
		return entryId;
	}

	public void setEntryId(String entryId) {
		this.entryId = entryId;
	}

	@Override
	protected int getTTL() {
		return AGGR_TTL;
	}

}

package com.kaltura.live.model.aggregation.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.Row;

/**
 *	This class is a base class representing the cassandra access object for live aggregation objects 
 */
public abstract class LiveEventDAO extends LiveDAO {
	
	public static final int AGGR_TTL = 60 * 60 * 3;
	
	// Equivalent to no TTL
	public static final int HOURLY_AGGR_TTL = 0;
	
	/** Auto generated serial UID */
	private static final long serialVersionUID = 3957419748277847064L;
	
	
	/** --- Object fields --- */
	
	 protected Date eventTime;
	 protected long alive;
	 protected long bitrate;
	 protected long bitrateCount;
	 protected long bufferTime;
	 protected long plays;

	/**
	 * Empty constructor
	 */
	public LiveEventDAO() {
		super();
	}
	
	/**
	 * Constructor based on reads event.
	 * @param row
	 */
	public LiveEventDAO(Row row) {
		super();
		this.eventTime = row.getDate("event_time");
		this.alive = row.getLong("alive");
		this.bitrate = row.getLong("bitrate");
		this.bitrateCount = row.getLong("bitrate_count");
		this.bufferTime = row.getLong("buffer_time");
		this.plays = row.getLong("plays");
	}
	
	/**
	 * @return The table name
	 */
	abstract protected String getTableName();
	
	/**
	 * @return List<String> the list of key fields
	 */
	protected List<String> getKeyFields() {
		return Arrays.asList(new String[]{"entry_id", "event_time"});
	}
	
	/**
	 * @return List<String> List of all fields which are table specific
	 */
	abstract protected List<String> getTableSpecificFields();

	/**
	 * @return List<String> List of all fields that are common between all live events tables
	 */
	protected List<String> getCommonFields() {
		return Arrays.asList(new String[]{"plays", "alive", "bitrate", "bitrate_count", "buffer_time"});
	}
	
	/**
	 * @return List<String> All fields within the cassandra table
	 */
	protected List<String> getTableFields() {
		List<String> fields = new ArrayList<String>();
		fields.addAll(getKeyFields());
		fields.addAll(getTableSpecificFields());
		fields.addAll(getCommonFields());
		return fields;
	}
	
	
	/** -- Getters and setters */

	public Date getEventTime() {
		return eventTime;
	}

	public void setEventTime(Date eventTime) {
		this.eventTime = eventTime;
	}

	public long getAlive() {
		return alive;
	}

	public void setAlive(long alive) {
		this.alive = alive;
	}

	public long getBitrate() {
		return bitrate;
	}

	public void setBitrate(long bitrate) {
		this.bitrate = bitrate;
	}

	public long getBitrateCount() {
		return bitrateCount;
	}

	public void setBitrateCount(long bitrateCount) {
		this.bitrateCount = bitrateCount;
	}

	public long getBufferTime() {
		return bufferTime;
	}

	public void setBufferTime(long bufferTime) {
		this.bufferTime = bufferTime;
	}

	public long getPlays() {
		return plays;
	}

	public void setPlays(long plays) {
		this.plays = plays;
	}
	
}

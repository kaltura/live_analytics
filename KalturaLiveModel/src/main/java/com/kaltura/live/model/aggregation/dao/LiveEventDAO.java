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
	
	public static final int AGGR_TTL = 60 * 60 * 36;
	
	// Equivalent to no TTL
	public static final int HOURLY_AGGR_TTL = 0;
	
	/** Auto generated serial UID */
	private static final long serialVersionUID = 3957419748277847064L;
	
	/** --- Object fields --- */
	
    protected Date eventTime;
    protected long alive;
    protected long bitrate;
    protected long bitrateCount;
    protected double bufferTime;
    protected long plays;

    protected long dvrAlive;

	/**
	 * Empty constructor
	 */
	public LiveEventDAO() {
		super();
	}
	
	/**
	 * Constructor based on reads event.
	 * @param row nothing
	 */
	public LiveEventDAO(Row row) {
		super();
		this.eventTime = new Date(row.getDate("event_time").getMillisSinceEpoch());
		this.alive = row.getLong("alive");
		this.bitrate = row.getLong("bitrate");
		this.bitrateCount = row.getLong("bitrate_count");
        /* Important! We are keeping buffer_time as a Cassandra counter which is limited to Long (thus we are
         * multiplying by 100 on set and divide by 100 on get)
         ***/
		this.bufferTime = row.getLong("buffer_time") / 100.0;
		this.plays = row.getLong("plays");
        this.dvrAlive = row.getLong("dvr_alive");
	}
	
	/**
	 * @return The table name
	 */
	abstract protected String getTableName();
	
	/**
	 * @return List<String> the list of key fields
	 */
	protected List<String> getKeyFields() {
		return Arrays.<String>asList("entry_id", "event_time");
	}
	
	/**
	 * @return List<String> List of all fields which are table specific
	 */
	abstract protected List<String> getTableSpecificFields();

	/**
	 * @return List<String> List of all fields that are common between all live events tables
	 */
	protected List<String> getCommonFields() {
		return Arrays.<String>asList("plays", "alive", "dvr_alive", "bitrate", "bitrate_count", "buffer_time");
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

    public long getDVRAlive() {
        return dvrAlive;
    }

    public void setDVRAlive(long dvrAlive) {
        this.dvrAlive = dvrAlive;
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
	
	public double getBufferTime() {
		return bufferTime;
	}
	
	public void setBufferTime(double bufferTime) {
		this.bufferTime = bufferTime;
	}

	public long getPlays() {
		return plays;
	}

	public void setPlays(long plays) {
		this.plays = plays;
	}
	
}

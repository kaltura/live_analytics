package com.kaltura.live.model.aggregation.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.StatsEvent;

/**
 *	This class is a base class representing the cassandra access object for live aggregation objects 
 */
public abstract class LiveEventDAO implements Serializable {

	/** Auto generated serial UID */
	private static final long serialVersionUID = 3957419748277847064L;
	
	private static Logger LOG = LoggerFactory.getLogger(LiveEventDAO.class);
	
	/** Prepared statement for cassandra update */
	protected PreparedStatement statement;
	
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
	
	/**
	 * @param session creates and initializes the update statement if needed.
	 */
	protected void createStatement(SerializableSession session) {
		if(statement == null) {
			List<String> fields = getTableFields();
			String fieldsStr = StringUtils.join(fields, ",");
			String[] values = new String[fields.size()];
			Arrays.fill(values, "?");
			String valuesStr = StringUtils.join(Arrays.asList(values), ",");
			statement = session.getSession().prepare("INSERT INTO " + getTableName() + " (" + fieldsStr + ") " +
					"VALUES (" + valuesStr + ")");
		}
	}
	
	/**
	 * Saves a single event to a given cassandra session
	 * @param session The session connecting to the cassandra
	 * @param event The event we'd like to save
	 */
	abstract public void saveOrUpdate(SerializableSession session, StatsEvent aggregatedResult);
}

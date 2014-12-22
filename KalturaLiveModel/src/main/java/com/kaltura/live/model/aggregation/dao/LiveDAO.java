package com.kaltura.live.model.aggregation.dao;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.StatsEvent;

public abstract class LiveDAO implements Serializable {

	private static final long serialVersionUID = 5605895483242146183L;
	
	public static final int RETRIES_NUM = 3;
	
	/** Prepared statement for cassandra update */
	protected PreparedStatement statement;
	
	/**
	 * Empty constructor
	 */
	public LiveDAO() {
		
	}
	
	/**
	 * Constructor based on reads event.
	 * @param row
	 */
	public LiveDAO(Row row) {
		
	}
	
	/**
	 * @return The table name
	 */
	abstract protected String getTableName();
	
	/**
	 * @return List<String> All fields within the cassandra table
	 */
	abstract protected List<String> getTableFields();
	
	
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
					"VALUES (" + valuesStr + ") USING TTL " + getTTL());
		}
	}
	
	/**
	 * Saves a single event to a given cassandra session
	 * @param session The session connecting to the cassandra
	 * @param event The event we'd like to save
	 */
	abstract public void saveOrUpdate(SerializableSession session, StatsEvent aggregatedResult);
	
	/**
	 * @return TTL to save row in seconds 
	 */
	abstract protected int getTTL();
	


}

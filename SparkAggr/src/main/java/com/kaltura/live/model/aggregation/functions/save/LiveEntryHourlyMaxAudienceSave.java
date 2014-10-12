package com.kaltura.live.model.aggregation.functions.save;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.StatsEvent;
import com.kaltura.live.model.aggregation.dao.LiveEntryEventDAO;
import com.kaltura.live.model.aggregation.dao.LiveEntryPeakDAO;
import com.kaltura.live.model.aggregation.dao.LiveEventDAO;
import com.kaltura.live.model.aggregation.keys.EntryHourlyKey;
import com.kaltura.live.model.aggregation.keys.EventKey;

public class LiveEntryHourlyMaxAudienceSave extends LiveEventSave {

	private static final long serialVersionUID = 1745611479417357511L;

	/** The session connecting to the cassandra instance */
	protected SerializableSession session;
	
	private static final String TABLE_NAME = "kaltura_live.hourly_live_events";
	
	/**
	 * Constructor
	 * @param session - The session connecting to the cassandra instance
	 */
	public LiveEntryHourlyMaxAudienceSave(SerializableSession session) {
		super(session);
	}

	@Override
	protected LiveEventDAO createLiveEventDAO() {
		return new LiveEntryPeakDAO(); 
	}
	
	

}

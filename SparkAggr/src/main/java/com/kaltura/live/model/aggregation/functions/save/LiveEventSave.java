package com.kaltura.live.model.aggregation.functions.save;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.StatsEvent;
import com.kaltura.live.model.aggregation.dao.LiveEventDAO;
import com.kaltura.live.model.aggregation.keys.EventKey;

/**
 *  This is base class represent a live aggregation save functionality
 */
public abstract class LiveEventSave extends FlatMapFunction<Iterator<Tuple2<EventKey, StatsEvent>>, Boolean> {

	private static final long serialVersionUID = 2988746438063952734L;
	
	/** The session connecting to the cassandra instance */
	protected SerializableSession session;
	
	/** Holds the live event object */
	protected LiveEventDAO event;
	
	/**
	 * Constructor
	 * @param session - The session connecting to the cassandra instance
	 */
	public LiveEventSave(SerializableSession session) {
		this.session = session;
		this.event = createLiveEventDAO();
	}
	
	/**
	 * @return LiveEventDAO live event cassandra's access object
	 */
	abstract protected LiveEventDAO createLiveEventDAO();
	
	/**
	 * @param it - Iterator over tuples of event key & stats event
	 */
	@Override
	public Iterable<Boolean> call(Iterator<Tuple2<EventKey, StatsEvent>> it) throws Exception {
		 while (it.hasNext()) {
			Tuple2<EventKey, StatsEvent> row = it.next(); 
			EventKey key = row._1;
			StatsEvent stats = row._2;
			key.manipulateStatsEventByKey(stats);
		    event.saveOrUpdate(this.session, stats);
		 }
		 
		 // TODO - we might want to return something that indicates the status of the update.
		 return new ArrayList<Boolean>();
	}
}
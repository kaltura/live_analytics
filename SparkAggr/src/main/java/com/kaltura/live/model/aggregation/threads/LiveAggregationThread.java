package com.kaltura.live.model.aggregation.threads;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.live.model.StatsEvent;
import com.kaltura.live.model.aggregation.functions.map.LiveEventMap;
import com.kaltura.live.model.aggregation.functions.reduce.LiveEventReduce;
import com.kaltura.live.model.aggregation.functions.save.LiveEventSave;
import com.kaltura.live.model.aggregation.keys.EventKey;

/**
 * This thread is responsible for aggregating results over a given event list and save them
 */
public class LiveAggregationThread implements Runnable, Serializable {

	private static final long serialVersionUID = 1636139741423548096L;
	
	private static Logger LOG = LoggerFactory.getLogger(LiveAggregationThread.class);
	
	/** This is a list of events to aggregate */
	protected JavaRDD<StatsEvent> events;
	
	/* -- function pointers -- */
	
	/** This class implements the map function */
	protected LiveEventMap mapFunction;
	
	/** This class implements the save function */
	protected LiveEventSave saveFunction;
	
	/** This class implements the reduce function */
	protected LiveEventReduce reduceFunction;
	
	public LiveAggregationThread(LiveEventMap aggrFunction, LiveEventReduce reduceFunction, LiveEventSave saveFunction) {
		this.mapFunction = aggrFunction;
		this.reduceFunction = reduceFunction;
		this.saveFunction = saveFunction;
	}
	
	public void init(JavaRDD<StatsEvent> events) {
		this.events = events; 
	}
	
	@Override
	public void run() {
		LOG.info("Start Thread");
		
		JavaPairRDD<EventKey, StatsEvent> eventByKeyMap = events.map(mapFunction);
		JavaPairRDD<EventKey, StatsEvent> mergedEventsByKey = eventByKeyMap.reduceByKey(reduceFunction);
		JavaRDD<Boolean> result = mergedEventsByKey.mapPartitions(saveFunction);
		
		result.count();
		LOG.info("Done Thread");
	}		 
	

}

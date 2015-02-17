package com.kaltura.live.model.aggregation.threads;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.live.model.aggregation.StatsEvent;
import com.kaltura.live.model.aggregation.filter.StatsEventsHourlyFilter;
import com.kaltura.live.model.aggregation.filter.StatsEventsFilter;
import com.kaltura.live.model.aggregation.functions.map.LiveEventMap;
import com.kaltura.live.model.aggregation.functions.reduce.LiveEventReduce;
import com.kaltura.live.model.aggregation.functions.save.LiveEventSave;
import com.kaltura.live.model.aggregation.keys.EventKey;

/**
 * This thread is responsible for aggregating results over a given event list and save them
 */
public abstract class LiveAggregationCycle implements /*Runnable,*/ Serializable {

	private static final long serialVersionUID = 1636139741423548096L;
	
	private static Logger LOG = LoggerFactory.getLogger(LiveAggregationCycle.class);
	
	private int iterCount = 0;
	/** This is a list of events to aggregate */
	protected JavaRDD<StatsEvent> events = null;
	
	/** Old events */
	protected JavaPairRDD<EventKey, StatsEvent> aggregatedEvents = null;
	
	/* -- function pointers -- */
	
	/** This class implements the map function */
	protected LiveEventMap mapFunction;
	
	/** This class implements the save function */
	protected LiveEventSave saveFunction;
	
	/** This class implements the reduce function */
	protected LiveEventReduce reduceFunction;
	
	public LiveAggregationCycle(LiveEventMap mapFunction, LiveEventReduce reduceFunction, LiveEventSave saveFunction) {
		this.mapFunction = mapFunction;
		this.reduceFunction = reduceFunction;
		this.saveFunction = saveFunction;
	}
	
	public void init(JavaRDD<StatsEvent> events) {
		this.events = events; 
	}
	
	//@Override
	public void run() {

		long startTime = System.currentTimeMillis();

	    JavaPairRDD<EventKey, StatsEvent> eventByKeyMap = events.mapToPair(mapFunction);

        if (aggregatedEvents != null) {
                eventByKeyMap = eventByKeyMap.union(aggregatedEvents);
        }

        JavaPairRDD<EventKey, StatsEvent> mergedEventsByKey = eventByKeyMap.reduceByKey(reduceFunction);


		mergedEventsByKey.count(); // added to force action for time calculation

		long endProcessTime = System.currentTimeMillis();

		LOG.info("LiveAggregationCycle " + "  process time (msec): " + (endProcessTime - startTime));

        JavaRDD<Boolean> result = mergedEventsByKey.mapPartitions(saveFunction);

        // filter old hours aggregated results
        mergedEventsByKey = mergedEventsByKey.filter(getFilterFunction());

		if (iterCount > 50) {
                result.checkpoint();
        }
        result.count();

		long endSaveTime = System.currentTimeMillis();

		LOG.info("LiveAggregationCycle " + "  save time (msec): " + (endSaveTime - startTime));

        if (aggregatedEvents != null)
                aggregatedEvents.unpersist();

        aggregatedEvents = mergedEventsByKey;
        aggregatedEvents.cache();
        if (iterCount > 50) {
                aggregatedEvents.checkpoint();
                iterCount = 0;
        }
        ++iterCount;
        aggregatedEvents.count();
       
	}
	
	protected abstract StatsEventsFilter getFilterFunction();
	

}

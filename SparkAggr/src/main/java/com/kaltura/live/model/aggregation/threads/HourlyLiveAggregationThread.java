package com.kaltura.live.model.aggregation.threads;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.kaltura.live.model.aggregation.StatsEvent;
import com.kaltura.live.model.aggregation.filter.StatsEventsFilter;
import com.kaltura.live.model.aggregation.functions.map.LiveEventMap;
import com.kaltura.live.model.aggregation.functions.reduce.LiveEventReduce;
import com.kaltura.live.model.aggregation.functions.save.LiveEventSave;
import com.kaltura.live.model.aggregation.keys.EventKey;

/**
 *	This thread is responsible to  
 */
public class HourlyLiveAggregationThread extends LiveAggregationThread {

	private static final long serialVersionUID = -6634875852998471397L;

	private static Logger LOG = LoggerFactory.getLogger(HourlyLiveAggregationThread.class);

	/** Old events */
	private JavaPairRDD<EventKey, StatsEvent> aggregatedEvents;
	
	public HourlyLiveAggregationThread(LiveEventMap aggrFunction, LiveEventReduce reduceFunction, LiveEventSave saveFunction) {
		super(aggrFunction, reduceFunction, saveFunction);
	}
	
	@Override
	public void run() {
		
		JavaPairRDD<EventKey, StatsEvent> eventByKeyMap = events.map(mapFunction);

		if (aggregatedEvents != null) {
			eventByKeyMap = eventByKeyMap.union(aggregatedEvents);
		}
		
		JavaPairRDD<EventKey, StatsEvent> mergedEventsByKey = eventByKeyMap.reduceByKey(reduceFunction);
		JavaRDD<Boolean> result = mergedEventsByKey.mapPartitions(saveFunction);
		// filter old hours aggregated results
		mergedEventsByKey = mergedEventsByKey.filter(new StatsEventsFilter());
		result.count();
		
		if (aggregatedEvents != null)		
			aggregatedEvents.unpersist();

		aggregatedEvents = mergedEventsByKey;
		aggregatedEvents.cache();
		aggregatedEvents.count();
	}		 
	
}

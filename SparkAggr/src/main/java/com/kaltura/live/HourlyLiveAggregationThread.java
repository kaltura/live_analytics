package com.kaltura.live;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HourlyLiveAggregationThread extends LiveAggregationThread {

	private static Logger LOG = LoggerFactory.getLogger(HourlyLiveAggregationThread.class);

	private JavaPairRDD<EventKey, StatsEvent> aggregated_events;
	public HourlyLiveAggregationThread(LiveAggregation aggrFunc, LiveAggrSaveFunction saveFunc) {
		super(aggrFunc, saveFunc);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void run() {
		LOG.info("Start Thread");
		
		JavaPairRDD<EventKey, StatsEvent> extractedByEntry = events.map(aggrFunc);

		if (aggregated_events != null) 
			extractedByEntry = extractedByEntry.union(aggregated_events);
		
		JavaPairRDD<EventKey, StatsEvent> countsByEntry = extractedByEntry.reduceByKey(new Function2<StatsEvent, StatsEvent, StatsEvent>() {
			 @Override
		     public StatsEvent call(StatsEvent stats, StatsEvent stats2) throws Exception {
				 return stats.merge(stats2);
		     }
		});
		    
		JavaRDD<Boolean> result = countsByEntry.mapPartitions(saveFunc);
		result.count();
		
		if (aggregated_events != null)		
			aggregated_events.unpersist();

		aggregated_events = countsByEntry;
		aggregated_events.cache();
		aggregated_events.count();
		
		LOG.info("Done Thread");
	}		 
	
	

}

package com.kaltura.live;

import java.io.Serializable;
import java.util.Iterator;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;



public class LiveAggregationThread implements Runnable, Serializable {

	protected JavaRDD<StatsEvent> events;
	protected LiveAggregation aggrFunc;
	protected LiveAggrSaveFunction saveFunc;
	
	private static Logger LOG = LoggerFactory.getLogger(LiveAggregationThread.class);
	
	public LiveAggregationThread(LiveAggregation aggrFunc, LiveAggrSaveFunction saveFunc) {
		this.aggrFunc = aggrFunc;
		this.saveFunc = saveFunc;
	
	}
	
	public void init(JavaRDD<StatsEvent> events) {
		
		this.events = events; 
	}
	@Override
	public void run() {
		LOG.info("Start Thread");
		JavaPairRDD<EventKey, StatsEvent> extractedByEntry = events.map(aggrFunc);

		JavaPairRDD<EventKey, StatsEvent> countsByEntry = extractedByEntry.reduceByKey(new Function2<StatsEvent, StatsEvent, StatsEvent>() {
			 @Override
		     public StatsEvent call(StatsEvent stats, StatsEvent stats2) throws Exception {
				 return stats.merge(stats2);
		     }
		});
		    
		JavaRDD<Boolean> result = countsByEntry.mapPartitions(saveFunc);
		result.count();
		LOG.info("Done Thread");
	}		 
	

}

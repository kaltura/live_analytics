package com.kaltura.live.model.aggregation.threads;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.kaltura.live.model.aggregation.StatsEvent;
import com.kaltura.live.model.aggregation.functions.map.LiveEntryAggrMap;
import com.kaltura.live.model.aggregation.functions.map.LiveEventMap;
import com.kaltura.live.model.aggregation.functions.reduce.LiveEventMaxAudience;
import com.kaltura.live.model.aggregation.functions.reduce.LiveEventReduce;
import com.kaltura.live.model.aggregation.functions.save.LiveEntryHourlyMaxAudienceSave;
import com.kaltura.live.model.aggregation.functions.save.LiveEventSave;
import com.kaltura.live.model.aggregation.keys.EventKey;

public class EntryRealTimeLiveAggregationCycle extends RealTimeLiveAggregationCycle {

	private static final long serialVersionUID = 2799690938410469735L;
	
	protected JavaPairRDD<EventKey, StatsEvent> peakAudienceEvent = null;
	
	private LiveEntryAggrMap aggrMapFunction;
	private LiveEventMaxAudience aggrReduceFunction; 
	private LiveEntryHourlyMaxAudienceSave aggrSaveFunction;
	
	public EntryRealTimeLiveAggregationCycle(LiveEventMap mapFunction,
			LiveEventReduce reduceFunction, LiveEventSave saveFunction, 
			LiveEntryAggrMap aggrMapFunction, LiveEventMaxAudience aggrReduceFunction, 
			LiveEntryHourlyMaxAudienceSave aggrSaveFunction) {
		super(mapFunction, reduceFunction, saveFunction);
		this.aggrMapFunction =  aggrMapFunction;
		this.aggrReduceFunction =  aggrReduceFunction;
		this.aggrSaveFunction =  aggrSaveFunction;
	}

	
	@Override
	public void run() {
		super.run();
		
		JavaPairRDD<EventKey, StatsEvent> audience = aggregatedEvents.mapToPair(aggrMapFunction);
		
		if (peakAudienceEvent != null) {
			audience = audience.union(peakAudienceEvent);
		}
		
		JavaPairRDD<EventKey, StatsEvent> topAudience = audience.reduceByKey(aggrReduceFunction);
		//topAudience.filter(new StatsEventsHourlyFilter());
		JavaRDD<Boolean> result = topAudience.mapPartitions(aggrSaveFunction);
		peakAudienceEvent = topAudience;
		result.count();
		
			 
	}

}

package com.kaltura.live.model.aggregation.threads;

import com.kaltura.live.model.aggregation.filter.StatsEventsHourlyFilter;
import com.kaltura.live.model.aggregation.filter.StatsEventsFilter;
import com.kaltura.live.model.aggregation.functions.map.LiveEventMap;
import com.kaltura.live.model.aggregation.functions.reduce.LiveEventReduce;
import com.kaltura.live.model.aggregation.functions.save.LiveEventSave;


/**
 *	This thread is responsible to  
 */
public class HourlyLiveAggregationCycle extends LiveAggregationCycle {

	private static final long serialVersionUID = -6634875852998471397L;
	
	public HourlyLiveAggregationCycle(LiveEventMap aggrFunction, LiveEventReduce reduceFunction, LiveEventSave saveFunction) {
		super(aggrFunction, reduceFunction, saveFunction);
	}

	@Override
	protected StatsEventsFilter getFilterFunction() {
		return new StatsEventsHourlyFilter();
	}
	
}

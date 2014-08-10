package com.kaltura.live.model.aggregation.threads;

import com.kaltura.live.model.aggregation.filter.StatsEventsFilter;
import com.kaltura.live.model.aggregation.filter.StatsEventsRealTimeFilter;
import com.kaltura.live.model.aggregation.functions.map.LiveEventMap;
import com.kaltura.live.model.aggregation.functions.reduce.LiveEventReduce;
import com.kaltura.live.model.aggregation.functions.save.LiveEventSave;

public class RealTimeLiveAggregationCycle extends LiveAggregationCycle {

	private static final long serialVersionUID = -2335303060005193307L;

	public RealTimeLiveAggregationCycle(LiveEventMap aggrFunction,
			LiveEventReduce reduceFunction, LiveEventSave saveFunction) {
		super(aggrFunction, reduceFunction, saveFunction);
	}

	@Override
	protected StatsEventsFilter getFilterFunction() {
		return new StatsEventsRealTimeFilter();
	}

}

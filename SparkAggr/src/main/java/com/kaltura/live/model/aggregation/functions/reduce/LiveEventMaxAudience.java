package com.kaltura.live.model.aggregation.functions.reduce;

import org.apache.spark.api.java.function.Function2;

import com.kaltura.live.model.aggregation.StatsEvent;


public class LiveEventMaxAudience implements Function2<StatsEvent, StatsEvent, StatsEvent> {

	private static final long serialVersionUID = 6275764970460440047L;

	@Override
	public StatsEvent call(StatsEvent stats, StatsEvent stats2) throws Exception {
        return stats.maxAudience(stats2);
		//return v1.getAlive() > v2.getAlive()  ? v1 : v2;
			
	} 

}

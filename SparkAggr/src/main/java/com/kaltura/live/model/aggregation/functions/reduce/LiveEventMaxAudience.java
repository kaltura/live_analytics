package com.kaltura.live.model.aggregation.functions.reduce;

import org.apache.spark.api.java.function.Function2;

import com.kaltura.live.model.aggregation.StatsEvent;


public class LiveEventMaxAudience implements Function2<StatsEvent, StatsEvent, StatsEvent> {

	private static final long serialVersionUID = 6275764970460440047L;

	@Override
	public StatsEvent call(StatsEvent v1, StatsEvent v2) throws Exception {
		// TODO Auto-generated method stub
		return v1.getAlive() + v1.getPlays() > v2.getAlive() + v2.getPlays() ? v1 : v2;
			
	} 

}

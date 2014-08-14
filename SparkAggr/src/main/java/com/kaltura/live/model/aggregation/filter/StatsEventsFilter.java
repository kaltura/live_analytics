package com.kaltura.live.model.aggregation.filter;

import java.util.Date;
import org.apache.spark.api.java.function.Function;

import com.kaltura.live.model.aggregation.StatsEvent;
import com.kaltura.live.model.aggregation.keys.EventKey;

import scala.Tuple2;

public abstract class StatsEventsFilter implements Function<Tuple2<EventKey, StatsEvent>, Boolean> {


	private static final long serialVersionUID = 1923660264190281156L;

	protected abstract Date getLatestTimeToSave();
	
	@Override
	public Boolean call(Tuple2<EventKey, StatsEvent> event) throws Exception {
		EventKey eventKey = event._1;
		
		if (eventKey.getEventTime().before(getLatestTimeToSave())) {
			return false;
		}
		
		return true; 
			
		
	}

}

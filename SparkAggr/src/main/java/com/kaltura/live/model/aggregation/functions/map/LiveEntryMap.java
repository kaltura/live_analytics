package com.kaltura.live.model.aggregation.functions.map;


import com.kaltura.live.model.aggregation.StatsEvent;
import com.kaltura.live.model.aggregation.keys.EntryKey;
import com.kaltura.live.model.aggregation.keys.EventKey;

import scala.Tuple2;

public class LiveEntryMap extends LiveEventMap {
	
	private static final long serialVersionUID = -7496119381362125224L;
	
	@Override
	public Tuple2<EventKey, StatsEvent> call(StatsEvent s) throws Exception {
		
		return new Tuple2<EventKey, StatsEvent>(new EntryKey(s.getEntryId(), s.getEventTime(), s.getPartnerId()) , s);
		
	}
	
}

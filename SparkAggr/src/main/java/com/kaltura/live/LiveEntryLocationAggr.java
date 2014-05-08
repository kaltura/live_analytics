package com.kaltura.live;

import scala.Tuple2;

public class LiveEntryLocationAggr extends LiveAggregation{
	//@Override
	public Tuple2<EventKey, StatsEvent> call(StatsEvent s) {
		return new Tuple2<EventKey, StatsEvent>(new EntryLocationKey(s.getEntryId(), s.getEventTime(), s.getPartnerId(), s.getCountry(), s.getCity()), s);
	}

}

package com.kaltura.live;

import java.util.ArrayList;
import java.util.Iterator;

import scala.Tuple2;

public class LiveEntryHourlyAggrSaveFunction extends LiveAggrSaveFunction {
	
	LiveEntryEventDAO event;
	
	public LiveEntryHourlyAggrSaveFunction(SerializableSession session) {
		event = new LiveEntryEventDAO(session, "kaltura_live.hourly_live_events");
	}
	
	@Override
	public Iterable<Boolean> call(Iterator<Tuple2<EventKey, StatsEvent>> it) throws Exception {
		 while (it.hasNext()) {
			Tuple2<EventKey, StatsEvent> row = it.next(); 
			EntryKey key = (EntryKey)row._1;
			StatsEvent stats = row._2;
			event.init(new StatsEvent(key.getEventTime(), 1, key.getEntryId(), null, null, null, stats.getPlays(), stats.getAlive(), stats.getBitrate(), stats.getBitrateCount(),stats.getBufferTime()));
		    event.saveOrUpdate();
		 }
		 return new ArrayList<Boolean>();
	}

}

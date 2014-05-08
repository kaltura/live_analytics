package com.kaltura.live;

import java.util.ArrayList;
import java.util.Iterator;

import scala.Tuple2;

public class LiveEntryAggrSaveFunction extends LiveAggrSaveFunction {
	
	LiveEntryEventDAO event;
	
	public LiveEntryAggrSaveFunction(SerializableSession session) {
		event = new LiveEntryEventDAO(session, "kaltura_live.live_events");
	}
	
	
	public void call2(Tuple2<EventKey, StatsEvent> row) throws Exception {
		EntryKey key = (EntryKey)row._1;
		StatsEvent stats = row._2;
		event.init(new StatsEvent(key.getEventTime(), 1, key.getEntryId(), null, null, stats.getPlays(), stats.getAlive(), stats.getBitrate(), stats.getBufferTime()));
    	event.saveOrUpdate();
		
	}
	
	@Override
	public Iterable<Boolean> call(Iterator<Tuple2<EventKey, StatsEvent>> it) throws Exception {
		 while (it.hasNext()) {
			Tuple2<EventKey, StatsEvent> row = it.next(); 
			EntryKey key = (EntryKey)row._1;
			StatsEvent stats = row._2;
			event.init(new StatsEvent(key.getEventTime(), 1, key.getEntryId(), null, null, stats.getPlays(), stats.getAlive(), stats.getBitrate(), stats.getBufferTime()));
		    event.saveOrUpdate();
		 }
		 return new ArrayList<Boolean>();
	}

}

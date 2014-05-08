package com.kaltura.live;

import java.util.ArrayList;
import java.util.Iterator;

import scala.Tuple2;

public class LiveEntryLocationAggrSaveFunction extends LiveAggrSaveFunction {
	
	private LiveEntryLocationEventDAO event;
	
	public LiveEntryLocationAggrSaveFunction(SerializableSession session) {
		event = new LiveEntryLocationEventDAO(session);
	}
	
	
	@Override
	public Iterable<Boolean> call(Iterator<Tuple2<EventKey, StatsEvent>> it) throws Exception {
		 while (it.hasNext()) {
			Tuple2<EventKey, StatsEvent> row = it.next(); 
			EntryLocationKey key = (EntryLocationKey)row._1;
			StatsEvent stats = row._2;
			event.init(new StatsEvent(key.getEventTime(), 1, key.getEntryId(), key.getCountry(), key.getCity(), stats.getPlays(), stats.getAlive(), stats.getBitrate(), stats.getBufferTime()));
		    event.saveOrUpdate();
		 }
		 return new ArrayList<Boolean>();
	}

}

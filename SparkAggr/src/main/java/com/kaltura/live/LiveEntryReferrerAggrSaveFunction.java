package com.kaltura.live;

import java.util.ArrayList;
import java.util.Iterator;

import scala.Tuple2;

public class LiveEntryReferrerAggrSaveFunction extends LiveAggrSaveFunction {
	
	private LiveEntryReferrerEventDAO event;
	
	public LiveEntryReferrerAggrSaveFunction(SerializableSession session) {
		event = new LiveEntryReferrerEventDAO(session);
	}
	
	
	@Override
	public Iterable<Boolean> call(Iterator<Tuple2<EventKey, StatsEvent>> it) throws Exception {
		 while (it.hasNext()) {
			Tuple2<EventKey, StatsEvent> row = it.next(); 
			EntryReferrerKey key = (EntryReferrerKey)row._1;
			StatsEvent stats = row._2;
			event.init(new StatsEvent(key.getEventTime(), 1, key.getEntryId(), null, null, key.getReferrer(), stats.getPlays(), stats.getAlive(), stats.getBitrate(), stats.getBitrateCount(), stats.getBufferTime()));
		    event.saveOrUpdate();
		 }
		 return new ArrayList<Boolean>();
	}

}

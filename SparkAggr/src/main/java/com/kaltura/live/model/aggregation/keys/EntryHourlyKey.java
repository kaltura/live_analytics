package com.kaltura.live.model.aggregation.keys;

import java.util.Date;

import com.kaltura.live.model.aggregation.StatsEvent;

public class EntryHourlyKey extends EntryKey {

	
	private static final long serialVersionUID = 2876814542975152288L;

	public EntryHourlyKey(String entryId, long eventTime, int partnerId) {
		super(entryId, eventTime, partnerId);
	}
	
	
	@Override
	public void manipulateStatsEventByKey(StatsEvent statsEvent) {
		// set event time to the key event time which is rounded to hour units.
		statsEvent.setEventTime(eventTime);
		
	}

}

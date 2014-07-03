package com.kaltura.live.model.aggregation.keys;

import java.util.Date;

import com.kaltura.live.model.StatsEvent;

public class PartnerHourlyKey extends PartnerKey {

	private static final long serialVersionUID = 135368621498875455L;

	public PartnerHourlyKey(int partnerId, Date eventTime) {
		super(partnerId, eventTime);
	}
	
	@Override
	public void manipulateStatsEventByKey(StatsEvent statsEvent) {
		// set event time to the key event time which is rounded to hour units.
		statsEvent.setEventTime(eventTime);
		
	}


}

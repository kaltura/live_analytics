package com.kaltura.live.model.aggregation.functions.map;

import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.model.StatsEvent;
import com.kaltura.live.model.aggregation.keys.EntryKey;
import com.kaltura.live.model.aggregation.keys.EventKey;

import scala.Tuple2;

public class LiveEntryHourlyMap extends LiveEventMap {
	
	private static final long serialVersionUID = -4496815986481100056L;

	@Override
	public Tuple2<EventKey, StatsEvent> call(StatsEvent s) {
		return new Tuple2<EventKey, StatsEvent>(new EntryKey(s.getEntryId(), DateUtils.roundHourDate(s.getEventTime()), s.getPartnerId()) , s);
	}
	
}

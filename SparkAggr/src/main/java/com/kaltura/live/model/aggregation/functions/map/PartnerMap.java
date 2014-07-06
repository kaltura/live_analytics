package com.kaltura.live.model.aggregation.functions.map;

import scala.Tuple2;

import com.kaltura.live.model.aggregation.StatsEvent;
import com.kaltura.live.model.aggregation.keys.EventKey;
import com.kaltura.live.model.aggregation.keys.PartnerKey;

public class PartnerMap extends LiveEventMap {

	private static final long serialVersionUID = -7826474647615345977L;

	@Override
	public Tuple2<EventKey, StatsEvent> call(StatsEvent s) throws Exception {
		return new Tuple2<EventKey, StatsEvent>(new PartnerKey(s.getPartnerId(), s.getEventTime()), s);
		
	}

}

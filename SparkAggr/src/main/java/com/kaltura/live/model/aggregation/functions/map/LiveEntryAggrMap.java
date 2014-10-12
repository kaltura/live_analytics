package com.kaltura.live.model.aggregation.functions.map;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.model.aggregation.StatsEvent;
import com.kaltura.live.model.aggregation.keys.EntryHourlyKey;
import com.kaltura.live.model.aggregation.keys.EventKey;

public class LiveEntryAggrMap implements PairFunction<Tuple2<EventKey, StatsEvent>, EventKey, StatsEvent> {

	private static final long serialVersionUID = 8590985877439140280L;

	@Override
	public Tuple2<EventKey, StatsEvent> call(Tuple2<EventKey, StatsEvent> t) throws Exception {
		StatsEvent event = t._2;
		EntryHourlyKey newKey= new EntryHourlyKey(event.getEntryId(),DateUtils.roundHourDate(event.getEventTime()),event.getPartnerId());
		return new Tuple2<EventKey, StatsEvent>(newKey, event);
	}
}

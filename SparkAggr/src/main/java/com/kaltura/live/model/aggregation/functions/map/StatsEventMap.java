package com.kaltura.live.model.aggregation.functions.map;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.kaltura.ip2location.SerializableIP2LocationReader;
import com.kaltura.live.infra.utils.LiveConfiguration;
import com.kaltura.live.model.aggregation.StatsEvent;

public class StatsEventMap implements FlatMapFunction<Iterator<String>, StatsEvent>{

	private static final long serialVersionUID = -61094768891844569L;

	@Override
	public Iterable<StatsEvent> call(Iterator<String> it) throws Exception {
		SerializableIP2LocationReader reader = new SerializableIP2LocationReader(
				LiveConfiguration.instance().getIp2locationPath());
		
		List<StatsEvent> statsEvents = new ArrayList<StatsEvent>();
		while (it.hasNext()) {
			String line = it.next();
			statsEvents.add(new StatsEvent(
					line, reader));
		}
		reader.close();
		return statsEvents;
	}

}

package com.kaltura.live.model.aggregation.filter;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.kaltura.live.SparkConfiguration;
import com.kaltura.live.infra.utils.DateUtils;

public class StatsEventsRealTimeFilter extends StatsEventsFilter {

	private static final long serialVersionUID = -1029156680788096287L;

	@Override
	protected Date getLatestTimeToSave() {
		return new Date(DateUtils.getCurrentMinInMillis() - TimeUnit.MINUTES.toMillis(SparkConfiguration.MINUTES_TO_SAVE));
	}

}

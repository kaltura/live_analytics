package com.kaltura.live.model.aggregation.filter;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.kaltura.live.SparkConfiguration;
import com.kaltura.live.infra.utils.DateUtils;

public class StatsEventHourlyFilter extends StatsEventsFilter {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4429274380151539441L;

	@Override
	protected Date getLatestTimeToSave() {
		return new Date(DateUtils.getCurrentHourInMillis() - TimeUnit.HOURS.toMillis(SparkConfiguration.HOURS_TO_SAVE));
	}

}

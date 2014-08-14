package com.kaltura.live.model.aggregation.filter;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.infra.utils.LiveConfiguration;

public class StatsEventsHourlyFilter extends StatsEventsFilter {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4429274380151539441L;

	@Override
	protected Date getLatestTimeToSave() {
		return new Date(DateUtils.getCurrentHourInMillis() - TimeUnit.HOURS.toMillis(LiveConfiguration.instance().getHoursToSave()));
	}

}

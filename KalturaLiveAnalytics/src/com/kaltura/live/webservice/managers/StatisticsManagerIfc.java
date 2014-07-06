package com.kaltura.live.webservice.managers;

import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

/**
 * Interface for statistics generation
 */
public interface StatisticsManagerIfc {
	
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	
	/**
	 * Abstract function. Each implementor is supposed to use the filter and return the matching live statistics
	 * @param filter The filter by which the results should be queried
	 * @return The matching live statistics
	 */
	abstract public LiveStatsListResponse query(LiveReportInputFilter filter);
	
}

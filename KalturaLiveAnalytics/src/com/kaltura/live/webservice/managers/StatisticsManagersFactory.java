package com.kaltura.live.webservice.managers;

import com.kaltura.live.webservice.model.LiveReportType;

/**
 *	Factory for statistics managers 
 */
final public class StatisticsManagersFactory {

	/**
	 * Returns the right statistics manager by required type.
	 * @param type The required report type
	 * @return The matching statistics manager
	 */
	public static final StatisticsManagerIfc getStatisticsManager(LiveReportType type) {
		switch (type) {
		case ENTRY_TOTAL:
			return new LiveReportStatsManager();

		default:
			return new StatisticsDummyImplementation();
		}
	}
}

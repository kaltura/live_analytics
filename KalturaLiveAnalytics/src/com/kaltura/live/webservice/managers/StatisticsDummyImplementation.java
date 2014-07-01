package com.kaltura.live.webservice.managers;

import com.kaltura.live.webservice.model.EntryLiveStats;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

/**
 * Dummy implementation so I can work...
 */
public class StatisticsDummyImplementation implements StatisticsManagerIfc {

	@Override
	public LiveStatsListResponse query(LiveReportInputFilter filter) {
		LiveStats[] arr = new LiveStats[1];
		arr[0] = new EntryLiveStats(3, 5, 57, 2, (float) 5.4, 11111, 11111, "zimsk");
		
		return new LiveStatsListResponse(arr, 1);
	}

}

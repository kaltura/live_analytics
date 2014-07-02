package com.kaltura.live.webservice;

import javax.jws.WebService;

import com.kaltura.live.webservice.managers.StatisticsManagerIfc;
import com.kaltura.live.webservice.managers.StatisticsManagersFactory;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportType;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

@WebService(endpointInterface="com.kaltura.live.webservice.LiveAnalytics")
public class LiveAnalyticsImpl implements LiveAnalytics{

	@Override
	public LiveStatsListResponse getReport(
			LiveReportType reportType,
			LiveReportInputFilter filter) {
		
		StatisticsManagerIfc manager = StatisticsManagersFactory.getStatisticsManager(reportType);
		return manager.query(filter);
	}
}
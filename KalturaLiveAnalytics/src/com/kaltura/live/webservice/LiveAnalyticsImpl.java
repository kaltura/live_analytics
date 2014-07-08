package com.kaltura.live.webservice;

import javax.jws.WebService;

import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportType;
import com.kaltura.live.webservice.model.LiveStatsListResponse;
import com.kaltura.live.webservice.reporters.BaseReporter;
import com.kaltura.live.webservice.reporters.ReportersFactory;

@WebService(endpointInterface="com.kaltura.live.webservice.LiveAnalytics")
public class LiveAnalyticsImpl implements LiveAnalytics{
	
	@Override
	public LiveStatsListResponse getReport(
			LiveReportType reportType,
			LiveReportInputFilter filter) {
		
		filter.validate();
		BaseReporter manager = ReportersFactory.getReporter(reportType);
		System.out.println("@_!! My lovely manager for report type " + reportType + " is : " + manager.getClass().getSimpleName());
		return manager.query(filter);
	}
}
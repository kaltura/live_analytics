package com.kaltura.live.webservice;

import javax.jws.WebService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.LiveEntriesListResponse;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportType;
import com.kaltura.live.webservice.model.LiveStatsListResponse;
import com.kaltura.live.webservice.reporters.BaseReporter;
import com.kaltura.live.webservice.reporters.LivePartnerEntryService;
import com.kaltura.live.webservice.reporters.ReportersFactory;

@WebService(endpointInterface="com.kaltura.live.webservice.LiveAnalytics")
public class LiveAnalyticsImpl implements LiveAnalytics{
	
	protected static Logger logger = LoggerFactory.getLogger(LiveAnalytics.class);
	
	@Override
	public LiveStatsListResponse getReport( LiveReportType reportType, LiveReportInputFilter filter) throws AnalyticsException {
		
		logger.debug("Live Analytics - Handling report request ");
		logger.debug("Report type : " + reportType);
		
		BaseReporter reporter = ReportersFactory.getReporter(reportType);

		// Filter validation
		filter.validate();
		reporter.validateFilter(filter);
		
		LiveStatsListResponse result = reporter.query(filter);
		
		logger.debug("Done.");
		return result;
			
	}

	@Override
	public LiveEntriesListResponse getLiveEntries(Integer partnerId) {
		logger.debug("Live Analytics - Handling get live entries request for partner id : " + partnerId);
		LivePartnerEntryService service = new LivePartnerEntryService();
		return service.getLiveEntries(partnerId);
	}
	
	
}
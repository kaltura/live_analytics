package com.kaltura.live.webservice;

import javax.annotation.Resource;
import javax.jws.WebService;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.LiveEntriesListResponse;
import com.kaltura.live.webservice.model.LiveEventsListResponse;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportPager;
import com.kaltura.live.webservice.model.LiveReportType;
import com.kaltura.live.webservice.model.LiveStatsListResponse;
import com.kaltura.live.webservice.reporters.BaseReporter;
import com.kaltura.live.webservice.reporters.EntryTimeLineReporter;
import com.kaltura.live.webservice.reporters.LivePartnerEntryService;
import com.kaltura.live.webservice.reporters.ReportersFactory;
import com.sun.xml.ws.api.message.Header;
import com.sun.xml.ws.api.message.HeaderList;
import com.sun.xml.ws.developer.JAXWSProperties;

@WebService(endpointInterface="com.kaltura.live.webservice.LiveAnalytics")
public class LiveAnalyticsImpl implements LiveAnalytics {

	protected static Logger logger = LoggerFactory.getLogger(LiveAnalytics.class);

	@Resource
	private WebServiceContext ctx;
	
	
	protected String getSession() {
		MessageContext context = ctx.getMessageContext();
		HeaderList hl = (HeaderList) context.get(JAXWSProperties.INBOUND_HEADER_LIST_PROPERTY);
		String sessionId = "UNKNWON";
		for (Header header : hl) {
			if(header.getLocalPart().equals("KALTURA_SESSION_ID")) {
				sessionId = header.getStringContent();
				break;
			}
		}
		MDC.put("sessionId", sessionId);
		return sessionId;
	}

	@Override
	public LiveStatsListResponse getReport( LiveReportType reportType, LiveReportInputFilter filter, LiveReportPager pager) throws AnalyticsException {
		
		logger.debug("------- " + getSession() +" -------");
		logger.debug("Live Analytics - Handling report request ");
		logger.debug("Report type : " + reportType);
		
		BaseReporter reporter = ReportersFactory.getReporter(reportType);

		// Filter validation
		filter.validate();
		reporter.validateFilter(filter);
		
		LiveStatsListResponse result = reporter.query(filter, pager);
		
		logger.debug("Done.");
		return result;
			
	}
	
	@Override
	public LiveEventsListResponse getEvents(LiveReportType reportType, LiveReportInputFilter filter, LiveReportPager pager)
			throws AnalyticsException {
		
		logger.debug("------- " + getSession() +" -------");
		logger.debug("Live Analytics - Handling event request ");
		logger.debug("Report type : " + reportType);
		
		if(reportType != LiveReportType.ENTRY_TIME_LINE)
			throw new RuntimeException(" Unsupported report type. " + reportType);
		
		EntryTimeLineReporter reporter = new EntryTimeLineReporter();
		// Filter validation
		filter.validate();
		reporter.validateFilter(filter);
		
		LiveEventsListResponse result = reporter.eventsQuery(filter, pager);
		
		logger.debug("Done.");
		return result;
	}

	@Override
	public LiveEntriesListResponse getLiveEntries(Integer partnerId) {
		logger.debug("------- " + getSession() +" -------");
		logger.debug("Live Analytics - Handling get live entries request for partner id : " + partnerId);
		LivePartnerEntryService service = new LivePartnerEntryService();
		return service.getLiveEntries(partnerId);
	}

}
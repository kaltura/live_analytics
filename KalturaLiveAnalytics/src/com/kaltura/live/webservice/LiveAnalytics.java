package com.kaltura.live.webservice;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import javax.jws.soap.SOAPBinding.Style;
import javax.xml.ws.RequestWrapper;
import javax.xml.ws.ResponseWrapper;

import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportType;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

@WebService
@SOAPBinding(style = Style.RPC)
public interface LiveAnalytics{
	
	@WebMethod 
	@RequestWrapper(localName = "getReportRequest")
	@ResponseWrapper(localName = "getReportResponse")
	public LiveStatsListResponse getReport(LiveReportType reportType, LiveReportInputFilter filter);
	
}
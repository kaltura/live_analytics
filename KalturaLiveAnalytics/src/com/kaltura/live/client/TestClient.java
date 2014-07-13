package com.kaltura.live.client;

import java.net.URL;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;

import com.kaltura.live.webservice.LiveAnalytics;
import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportType;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

public class TestClient{
	
	public static void main(String[] args) throws Exception {
	   
		
		URL url = new URL("http://pa-erans:9090/KalturaLiveAnalytics/KalturaLiveAnalytics?wsdl");
        QName qname = new QName("http://webservice.live.kaltura.com/", "LiveAnalyticsImplService");

        Service service = Service.create(url, qname);
        LiveAnalytics hello = service.getPort(LiveAnalytics.class);
        
        testTimeLine(hello);
    }
	
	private static void testTimeLine(LiveAnalytics hello) throws AnalyticsException {
		LiveReportType reportType = LiveReportType.ENTRY_TIME_LINE;
		LiveReportInputFilter filter = new LiveReportInputFilter();
		filter.setEntryIds("1_lcn2avg8");
		filter.setFromTime(1387100000);
		filter.setToTime(1387200000);
		
		LiveStatsListResponse z = hello.getReport(reportType, filter);
        printResult(reportType, z);
	}
	
	private static void printResult(LiveReportType reportType, LiveStatsListResponse z) {
		System.out.println("reportType: " + reportType);
		System.out.println("Total count : " + z.getTotalCount());
        
        int id = 1;
        for (LiveStats liveStats : z.getEvents()) {
			System.out.println("\tid:[" + id++ + "]" + liveStats.getBufferTime());
		}
	}
	
	

}

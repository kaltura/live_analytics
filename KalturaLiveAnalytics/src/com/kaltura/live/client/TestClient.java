package com.kaltura.live.client;

import java.net.URL;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;

import com.kaltura.live.webservice.LiveAnalytics;
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
        

        LiveReportType reportType = LiveReportType.ENTRY_SYNDICATION_TOTAL;
		LiveReportInputFilter filter = new LiveReportInputFilter(3, false, "1_eer", 0, 5, "+zim");
		LiveStatsListResponse z = hello.getReport(reportType, filter);
        System.out.println(z.getTotalCount());
        
        for (LiveStats liveStats : z.getEvents()) {
			System.out.println(liveStats.getPlays());
		}
       
    }

}

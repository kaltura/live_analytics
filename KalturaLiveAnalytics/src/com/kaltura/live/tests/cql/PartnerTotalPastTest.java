package com.kaltura.live.tests.cql;

import java.util.Calendar;

import junit.framework.Assert;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;
import com.kaltura.live.webservice.reporters.PartnerTotalReporter;

public class PartnerTotalPastTest extends BaseReporterTest {
	
	private class PartnerTotalReporterMock extends PartnerTotalReporter {
		public PartnerTotalReporterMock(Session sessionIn) {
			session = new SerializableSessionMock(sessionIn);
		}
	}

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(RESOURCE_DIR + "hourly_live_events_partner.cql","kaltura_live"));

    protected LiveReportInputFilter createFilter() {
    	Calendar now = DateUtils.getCurrentTime();
    	Calendar hourAgo = Calendar.getInstance();
    	hourAgo.setTime(now.getTime());
    	hourAgo.add(Calendar.HOUR, -2);
    	
		LiveReportInputFilter filter = new LiveReportInputFilter();
		filter.setPartnerId(777);
		filter.setToTime(now.getTime().getTime() / 1000);
		filter.setFromTime(hourAgo.getTime().getTime() / 1000);
		filter.setLive(false);
		return filter;
    }
    
    @Test
    public void testPartnerTotalPast() throws Exception {
    	
    	setTime();
    	PartnerTotalReporter reporter = new PartnerTotalReporterMock(cassandraCQLUnit.session);
    	LiveStatsListResponse results = reporter.query(createFilter(), null);
    	
    	Assert.assertEquals(1, results.getTotalCount());
    	LiveStats event = results.getObjects()[0];
    	Assert.assertEquals(7, event.getPlays());
    }
}

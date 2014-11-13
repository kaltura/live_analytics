package com.kaltura.live.tests.cql;

import java.util.Calendar;

import junit.framework.Assert;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.webservice.model.EntryLiveStats;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStatsListResponse;
import com.kaltura.live.webservice.reporters.EntryTotalReporter;

public class EntryTotalPastTest extends BaseReporterTest {
	
	private class EntryTotalReporterMock extends EntryTotalReporter {
		public EntryTotalReporterMock(Session sessionIn) {
			session = new SerializableSessionMock(sessionIn);
		}
	}

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(RESOURCE_DIR + "hourly_live_events.cql","kaltura_live"));

    protected LiveReportInputFilter createFilter() {
    	Calendar now = DateUtils.getCurrentTime();
    	Calendar hourAgo = Calendar.getInstance();
    	hourAgo.setTime(now.getTime());
    	hourAgo.add(Calendar.HOUR, -2);
    	
		LiveReportInputFilter filter = new LiveReportInputFilter();
		filter.setEntryIds("test_entry");
		filter.setToTime(now.getTimeInMillis() / 1000);
		filter.setFromTime(hourAgo.getTimeInMillis() / 1000);
		filter.setLive(false);
		return filter;
    }
    
    @Test
    public void testEntryTotalPast() throws Exception {
    	
    	setTime();
    	
    	EntryTotalReporter reporter = new EntryTotalReporterMock(cassandraCQLUnit.session);
    	LiveStatsListResponse results = reporter.query(createFilter(), null);
    	
    	Assert.assertEquals(1, results.getTotalCount());
    	EntryLiveStats event = (EntryLiveStats)results.getObjects()[0];
    	Assert.assertEquals(7, event.getPlays());
    	Assert.assertEquals(700, event.getPeakAudience());
    }
}

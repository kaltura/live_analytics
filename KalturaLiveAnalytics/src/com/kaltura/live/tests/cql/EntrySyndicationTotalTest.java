package com.kaltura.live.tests.cql;

import java.util.Calendar;

import junit.framework.Assert;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.webservice.model.EntryReferrerLiveStats;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportPager;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;
import com.kaltura.live.webservice.reporters.EntrySyndicationTotalReporter;

public class EntrySyndicationTotalTest extends BaseReporterTest {
	
	private class EntrySyndicationTotalReporterMock extends EntrySyndicationTotalReporter {
		public EntrySyndicationTotalReporterMock(Session sessionIn) {
			session = new SerializableSessionMock(sessionIn);
		}
	}

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(RESOURCE_DIR + "live_events_referrer.cql","kaltura_live"));

    protected LiveReportInputFilter createFilter() {
    	Calendar now = DateUtils.getCurrentTime();
    	Calendar hourAgo = Calendar.getInstance();
    	hourAgo.setTime(now.getTime());
    	hourAgo.add(Calendar.HOUR, -2);
    	
		LiveReportInputFilter filter = new LiveReportInputFilter();
		filter.setEntryIds("test_entry");
		filter.setToTime(now.getTime().getTime() / 1000);
		filter.setFromTime(hourAgo.getTime().getTime() / 1000);
		return filter;
    }
    
    @Test
    public void testEntrySyndicationTotal() throws Exception {
    	
    	setTime();
    	LiveReportPager pager = new LiveReportPager(2,1);
    	
    	EntrySyndicationTotalReporter reporter = new EntrySyndicationTotalReporterMock(cassandraCQLUnit.session);
    	LiveStatsListResponse results = reporter.query(createFilter(), pager);
    	
    	Assert.assertEquals(2, results.getTotalCount());
    	
    	LiveStats[] events = results.getObjects();
    	verifyReferrer(events, 0, "testC", 200);
    	verifyReferrer(events, 1, "testB", 100); 	
    }

	private void verifyReferrer(LiveStats[] events, int i, String name,
			int plays) {
		EntryReferrerLiveStats event = (EntryReferrerLiveStats)events[i]; 
    	Assert.assertEquals(name, event.getReferrer());
    	Assert.assertEquals(plays, event.getPlays());
	}
    
}

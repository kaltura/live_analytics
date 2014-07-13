package com.kaltura.live.tests.cql;

import junit.framework.Assert;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.kaltura.live.webservice.model.EntryReferrerLiveStats;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
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
		LiveReportInputFilter filter = new LiveReportInputFilter();
		filter.setEntryIds("test_entry");
		filter.setHoursBefore(1);
		return filter;
    }
    
    @Test
    public void should_have_started_and_execute_cql_script() throws Exception {
    	// TODO - add limit (no more than X returned results)
    	EntrySyndicationTotalReporter reporter = new EntrySyndicationTotalReporterMock(cassandraCQLUnit.session);
    	LiveStatsListResponse results = reporter.query(createFilter());
    	
    	Assert.assertEquals(3, results.getTotalCount());
    	
    	LiveStats[] events = results.getEvents();
    	verifyReferrer(events, 0, "testC", 200);
    	verifyReferrer(events, 1, "testB", 100); 	
    	verifyReferrer(events, 2, "testA", 6);
    }

	private void verifyReferrer(LiveStats[] events, int i, String name,
			int plays) {
		EntryReferrerLiveStats event = (EntryReferrerLiveStats)events[i]; 
    	Assert.assertEquals(name, event.getReferrer());
    	Assert.assertEquals(plays, event.getPlays());
	}
    
}

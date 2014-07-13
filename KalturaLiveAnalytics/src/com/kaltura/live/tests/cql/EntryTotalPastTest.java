package com.kaltura.live.tests.cql;

import junit.framework.Assert;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStats;
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
		LiveReportInputFilter filter = new LiveReportInputFilter();
		filter.setEntryIds("test_entry");
		filter.setHoursBefore(1);
		filter.setLive(false);
		return filter;
    }
    
    @Test
    public void should_have_started_and_execute_cql_script() throws Exception {
    	EntryTotalReporter reporter = new EntryTotalReporterMock(cassandraCQLUnit.session);
    	LiveStatsListResponse results = reporter.query(createFilter());
    	
    	Assert.assertEquals(1, results.getTotalCount());
    	LiveStats event = results.getEvents()[0];
    	Assert.assertEquals(7, event.getPlays());
    }
}

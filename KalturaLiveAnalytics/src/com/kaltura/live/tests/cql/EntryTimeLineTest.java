package com.kaltura.live.tests.cql;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStatsListResponse;
import com.kaltura.live.webservice.reporters.EntryTimeLineReporter;

public class EntryTimeLineTest extends BaseReporterTest{
	
	private class EntryTimeLineReporterMock extends EntryTimeLineReporter {
		public EntryTimeLineReporterMock(Session sessionIn) {
			session = new SerializableSessionMock(sessionIn);
		}
	}

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(RESOURCE_DIR + "live_events.cql","kaltura_live"));

    protected LiveReportInputFilter createFilter() {
    	LiveReportInputFilter filter = new LiveReportInputFilter();
		filter.setEntryIds("test_entry");
		filter.setFromTime(1387100000);
		filter.setToTime(1387200000);
		return filter;
    }
    
    @Test
    public void should_have_started_and_execute_cql_script() throws Exception {
    	EntryTimeLineReporter reporter = new EntryTimeLineReporterMock(cassandraCQLUnit.session);
    	LiveStatsListResponse results = reporter.query(createFilter());
    	
    	Assert.assertEquals(4, results.getTotalCount());
    }
    
}

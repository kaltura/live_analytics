package com.kaltura.live.tests.cql;

import junit.framework.Assert;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStatsListResponse;
import com.kaltura.live.webservice.reporters.EntryTotalReporter;

public class EntryTotalLiveTest extends BaseReporterTest {
	
	private class EntryTotalReporterMock extends EntryTotalReporter {
		public EntryTotalReporterMock(Session sessionIn) {
			session = new SerializableSessionMock(sessionIn);
		}
	}

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(RESOURCE_DIR + "live_events.cql","kaltura_live"));

    protected LiveReportInputFilter createFilter() {
		LiveReportInputFilter filter = new LiveReportInputFilter();
		filter.setEntryIds("test_entry");
		filter.setFromTime(DateUtils.getCurrentTime().getTimeInMillis() / 1000);
		filter.setToTime(DateUtils.getCurrentTime().getTimeInMillis() / 1000);
		filter.setLive(true);
		return filter;
    }
    
    @Test
    public void testEntryTotalLive() throws Exception {
    	setTime();
    	
    	EntryTotalReporter reporter = new EntryTotalReporterMock(cassandraCQLUnit.session);
    	LiveStatsListResponse results = reporter.query(createFilter(), null);
    	
    	Assert.assertEquals(1, results.getTotalCount());
    }
}

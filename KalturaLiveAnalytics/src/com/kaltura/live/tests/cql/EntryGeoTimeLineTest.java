package com.kaltura.live.tests.cql;

import junit.framework.Assert;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.kaltura.live.webservice.model.GeoTimeLiveStats;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;
import com.kaltura.live.webservice.reporters.EntryGeoTimeLineReporter;

public class EntryGeoTimeLineTest extends BaseReporterTest {
	
	private class EntryGeoTimeLineReporterMock extends EntryGeoTimeLineReporter {
		public EntryGeoTimeLineReporterMock(Session sessionIn) {
			session = new SerializableSessionMock(sessionIn);
		}
	}

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(RESOURCE_DIR + "live_events_location.cql","kaltura_live"));

    protected LiveReportInputFilter createFilter() {
		LiveReportInputFilter filter = new LiveReportInputFilter();
		filter.setEntryIds("test_entry");
		filter.setEventTime(1387121900);
		return filter;
    }
    
    @Test
    public void testEntryGeoTimeLineTest() throws Exception {
    	EntryGeoTimeLineReporter reporter = new EntryGeoTimeLineReporterMock(cassandraCQLUnit.session);
    	LiveStatsListResponse results = reporter.query(createFilter(), null);
    	
    	Assert.assertEquals(3, results.getTotalCount());
    	LiveStats[] events = results.getObjects();
    	testCountryAndCity((GeoTimeLiveStats) events[0], "-", "-");
    	testCountryAndCity((GeoTimeLiveStats) events[1], "-", "AFGHANISTAN");
    	
    	GeoTimeLiveStats event2 = (GeoTimeLiveStats) events[2];
    	testCountryAndCity(event2, "CHARIKAR", "AFGHANISTAN");
    	Assert.assertTrue(event2.getCountry().getLatitude() > 0);
    	Assert.assertTrue(event2.getCity().getLongitude() > 0);
    }
    
    protected void testCountryAndCity(GeoTimeLiveStats event, String city, String country) {
    	Assert.assertEquals(city, event.getCity().getName());
    	Assert.assertEquals(country, event.getCountry().getName());
    }
}

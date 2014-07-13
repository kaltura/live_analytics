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
    public void should_have_started_and_execute_cql_script() throws Exception {
    	EntryGeoTimeLineReporter reporter = new EntryGeoTimeLineReporterMock(cassandraCQLUnit.session);
    	LiveStatsListResponse results = reporter.query(createFilter());
    	
    	Assert.assertEquals(3, results.getTotalCount());
    	Assert.assertTrue(testCountryAndCity(results, "-", "-"));
    	Assert.assertTrue(testCountryAndCity(results, "-", "AFGHANISTAN"));
    	Assert.assertTrue(testCountryAndCity(results, "CHARIKAR", "AFGHANISTAN"));
    }
    
    protected boolean testCountryAndCity(LiveStatsListResponse results, String city, String country) {
    	for (LiveStats stat : results.getEvents()) {
    		GeoTimeLiveStats geoStat = (GeoTimeLiveStats)stat;
    		if(geoStat.getCity().getName().equals(city) && geoStat.getCountry().getName().equals(country))
    			return true;
		}
    	return false;
    }
}

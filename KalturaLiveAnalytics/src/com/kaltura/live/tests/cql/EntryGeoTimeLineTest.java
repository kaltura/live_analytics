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
		filter.setFromTime(1387100000);
		filter.setToTime(1387121910);
		return filter;
    }
    
    @Test
    public void testEntryGeoTimeLineTest() throws Exception {
    	EntryGeoTimeLineReporter reporter = new EntryGeoTimeLineReporterMock(cassandraCQLUnit.session);
    	LiveStatsListResponse results = reporter.query(createFilter(), null);
    	
    	Assert.assertEquals(4, results.getTotalCount());
    	LiveStats[] events = results.getObjects();
    	
    	boolean foundBoth = false, foundCountry = false;
    	int foundNone = 0;
    	for (LiveStats event : events) {
    		GeoTimeLiveStats geoEvent = (GeoTimeLiveStats)event;
    		String country = geoEvent.getCountry().getName();
    		String city = geoEvent.getCity().getName();
    		
			if(country.equals("AFGHANISTAN")) {
				if(city.equals("CHARIKAR")) {
					Assert.assertTrue(geoEvent.getCountry().getLatitude() > 0);
			    	Assert.assertTrue(geoEvent.getCity().getLongitude() > 0);
			    	foundBoth = true;
				} else if (city.equals("-")) {
					foundCountry = true;
				}
			} else if(country.equals("-") && city.equals("-")) {
				foundNone++;
			}
		}
    	
    	Assert.assertTrue(foundBoth);
    	Assert.assertTrue(foundCountry);
    	Assert.assertEquals(2, foundNone);
    }
    
    protected void testCountryAndCity(GeoTimeLiveStats event, String city, String country) {
    	Assert.assertEquals(city, event.getCity().getName());
    	Assert.assertEquals(country, event.getCountry().getName());
    }
}

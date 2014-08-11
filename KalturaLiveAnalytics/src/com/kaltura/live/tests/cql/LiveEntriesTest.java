package com.kaltura.live.tests.cql;

import junit.framework.Assert;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.kaltura.live.webservice.model.LiveEntriesListResponse;
import com.kaltura.live.webservice.reporters.LivePartnerEntryService;

public class LiveEntriesTest extends BaseReporterTest {
	
	private class LivePartnerEntryServiceMock extends LivePartnerEntryService {
		public LivePartnerEntryServiceMock(Session sessionIn) {
			session = new SerializableSessionMock(sessionIn);
		}
	}

	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(RESOURCE_DIR + "live_partner_entry.cql", "kaltura_live"));

	@Test
	public void testLiveEntries() throws Exception {
		LivePartnerEntryService sevice = new LivePartnerEntryServiceMock(cassandraCQLUnit.session);
		LiveEntriesListResponse result = sevice.getLiveEntries(777);

		Assert.assertNotNull(result);
		Assert.assertEquals(2, result.getTotalCount());
		Assert.assertNotNull(result.getEntries());
		Assert.assertEquals(2, result.getEntries().length);
		Assert.assertEquals("test_entry1", result.getEntries()[0]);
		Assert.assertEquals("test_entry3", result.getEntries()[1]);
	}

}

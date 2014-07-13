package com.kaltura.live.tests.cql;

import com.datastax.driver.core.Session;
import com.kaltura.live.infra.cache.SerializableSession;

abstract public class BaseReporterTest {
	
	protected static final String RESOURCE_DIR = "com/kaltura/live/tests/resources/";
	
	protected class SerializableSessionMock extends SerializableSession {
		public SerializableSessionMock(Session inSession) {
			this.session = inSession;
		}
	}

}

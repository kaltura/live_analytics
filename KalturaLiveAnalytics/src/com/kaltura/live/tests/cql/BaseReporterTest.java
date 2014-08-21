package com.kaltura.live.tests.cql;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Before;

import com.datastax.driver.core.Session;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.infra.utils.LiveConfiguration;

abstract public class BaseReporterTest {
	
	protected static final String RESOURCE_DIR = "com/kaltura/live/tests/resources/";
	protected static final String TESTING_NODE_NAME = "pa-erans";
	
	@Before
	public void testSetup() {
		LiveConfiguration.instance().setCassandraNodeName(TESTING_NODE_NAME);
	}
	
	protected class SerializableSessionMock extends SerializableSession {
		public SerializableSessionMock(Session inSession) {
			this.session = inSession;
		}
	}
	
	protected void setTime() {
    	
		try {
			SimpleDateFormat formatDate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
	    	Date time = formatDate.parse("15/Dec/2013:11:30:00 -0500");
	    	DateUtils.setCurrentTime(time);
		} catch (ParseException e) {
			throw new RuntimeException("Failed to set time");
		}
    	
	}

}

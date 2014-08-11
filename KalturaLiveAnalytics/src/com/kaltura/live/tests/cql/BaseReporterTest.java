package com.kaltura.live.tests.cql;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Before;

import com.datastax.driver.core.Session;
import com.kaltura.live.Configuration;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.infra.utils.DateUtils;

abstract public class BaseReporterTest {
	
	protected static final String RESOURCE_DIR = "com/kaltura/live/tests/resources/";
	
	@Before
	public void testSetup() {
		Configuration.NODE_NAME = "pa-erans";
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

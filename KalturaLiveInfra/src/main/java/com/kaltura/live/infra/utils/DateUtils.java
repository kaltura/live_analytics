package com.kaltura.live.infra.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(DateUtils.class);
	
	private static final String DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";

	public static Date roundDate(String eventDate) {
		  SimpleDateFormat formatDate = new SimpleDateFormat(DATE_FORMAT);
			
			try {
				Date date = formatDate.parse(eventDate);
				Calendar c = new GregorianCalendar();
				c.setTime(date);
				int seconds = c.get(Calendar.SECOND);
				int decSeconds = seconds / 10 * 10;
				c.set(Calendar.SECOND, decSeconds);
				return c.getTime();
			} catch (ParseException e) {
				LOG.error("failed to round date", e);
			}
			return null;
		
	}
	
	public static Date roundDate(Date eventDate) {
		Calendar c = new GregorianCalendar();
		c.setTime(eventDate);
		int seconds = c.get(Calendar.SECOND);
		int decSeconds = seconds / 10 * 10;
		c.set(Calendar.SECOND, decSeconds);
		return c.getTime();
	}
	
	public static Date roundHourDate(Date eventDate) {
		Calendar c = new GregorianCalendar();
		c.setTime(eventDate);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MINUTE, 0);
		return c.getTime();
	}
	
	public static Calendar getCurrentTime() {
		SimpleDateFormat formatDate = new SimpleDateFormat(DATE_FORMAT);
		
		Calendar cal = Calendar.getInstance();
		// TODO - remove hack
		try {
			cal.setTime(formatDate.parse("15/Dec/2013:11:32:00 -0500"));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return cal;
	}
}

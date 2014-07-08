package com.kaltura.live.infra.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(DateUtils.class);
	
	private static final String DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";
	
	// TODO discuss with orly timestamp to query issues.

	public static Date roundDate(String eventDate) {
		  SimpleDateFormat formatDate = new SimpleDateFormat(DATE_FORMAT);
			
			try {
				Date date = formatDate.parse(eventDate);
				return roundDate(date);
			} catch (ParseException e) {
				LOG.error("failed to round date", e);
			}
			return null;
		
	}
	
	public static Date roundDate(Date eventDate) {
		Calendar c = Calendar.getInstance();
		c.setTime(eventDate);
		int seconds = c.get(Calendar.SECOND);
		int decSeconds = seconds / 10 * 10;
		c.set(Calendar.SECOND, decSeconds);
		return c.getTime();
	}
	
	public static Date roundDate(long dateLong) {
		Date date = new Date(dateLong);
		return roundDate(date);
	}
	
	public static Date roundHourDate(Date eventDate) {
		Calendar c = Calendar.getInstance();
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
			cal.setTime(formatDate.parse("15/Dec/2013:11:30:00 -0500"));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return cal;
	}
	
	public static long getCurrentHourInMillis() {
		Calendar cal = Calendar.getInstance();
		// TODO - remove hack
		cal.setTimeInMillis(1387101600000L);
		
	  	cal.set(Calendar.MINUTE, 0);
	  	cal.set(Calendar.SECOND, 0);
	  	
	  	return cal.getTimeInMillis();
	}
	
	// TODO - remove hack
	public static long getCurrentHourInMillis(long startTime) {
		Calendar cal = Calendar.getInstance();
		long now = System.currentTimeMillis();
		if (now - startTime < 30*60*1000) {
			cal.setTimeInMillis(1387101600000L);
		} else {
			cal.setTimeInMillis(1387105200000L);
		}
		cal.set(Calendar.MINUTE, 0);
	  	cal.set(Calendar.SECOND, 0);
	  	return cal.getTimeInMillis();
	}
}

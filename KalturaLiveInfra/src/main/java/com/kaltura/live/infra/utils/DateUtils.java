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
	
	private static Calendar testCalendar = null;
	
	// TODO discuss with orly timestamp to query issues.

	public static Date roundDate(String eventDate, int centeringOffset) {
		  SimpleDateFormat formatDate = new SimpleDateFormat(DATE_FORMAT);
			
			try {
				Date date = formatDate.parse(eventDate);
				return roundDate(date, centeringOffset);
			} catch (ParseException e) {
				LOG.error("failed to round date", e);
			}
			return null;
		
	}
	
	public static Date roundDate(Date eventDate, int centeringOffset ) {
		Calendar c = Calendar.getInstance();
		c.setTime(eventDate);
		c.add(Calendar.SECOND, centeringOffset);
		
		int seconds = c.get(Calendar.SECOND);
		int decSeconds = seconds / 10 * 10;
		c.set(Calendar.SECOND, decSeconds);
		c.set(Calendar.MILLISECOND, 0);
		
		return c.getTime();
	}
	
	public static Date roundDate(long dateLong, int centeringOffset) {
		Date date = new Date(dateLong);
		return roundDate(date, centeringOffset);
	}
	
	public static Date roundDate(String eventDate) {
		  SimpleDateFormat formatDate = new SimpleDateFormat(DATE_FORMAT);
			
			try {
				Date date = formatDate.parse(eventDate);
				return roundDate(date, 0);
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
		c.set(Calendar.MILLISECOND, 0);
		
		return c.getTime();
	}
	
	public static Date roundDate(long dateLong) {
		Date date = new Date(dateLong * 1000);
		return roundDate(date, 0);
	}
	
	public static Date roundHourDate(Date eventDate) {
		Calendar c = Calendar.getInstance();
		c.setTime(eventDate);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTime();
	}
	
	public static long getCurrentHourInMillis() {
		Calendar cal = getCurrentTime();
		
	  	cal.set(Calendar.MINUTE, 0);
	  	cal.set(Calendar.SECOND, 0);
	  	cal.set(Calendar.MILLISECOND, 0);
	  	
	  	return cal.getTimeInMillis();
	}
	
	public static long getCurrentMinInMillis() {
		Calendar cal = Calendar.getInstance();
	
	  	cal.set(Calendar.SECOND, 0);
	  	cal.set(Calendar.MILLISECOND, 0);
	  	
	  	return cal.getTimeInMillis();
	}
	
	public static long getCurrentHourInMillis(long startTime) {
		Calendar cal = getCurrentTime();
		cal.set(Calendar.MINUTE, 0);
	  	cal.set(Calendar.SECOND, 0);
	  	cal.set(Calendar.MILLISECOND, 0);
	  	return cal.getTimeInMillis();
	}
	
	public static Calendar getCurrentTime() {
		if(testCalendar != null)
			return testCalendar;
		return Calendar.getInstance();
	}
	
	public static void setCurrentTime(Date date) {
		testCalendar = Calendar.getInstance();
		testCalendar.setTime(date);
	}
}

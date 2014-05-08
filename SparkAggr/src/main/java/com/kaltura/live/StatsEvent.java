package com.kaltura.live;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class StatsEvent implements Serializable {

	
	private static final long serialVersionUID = 2087162345237773624L;
	
	public static Pattern apacheLogRegex = Pattern.compile(
			"^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{5})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) (\\d+\\/\\d+) \"([^\"]+)\" \"([^\"]+)\".*");
	
	
	private Date eventTime;
	private int partnerId;
	private String entryId;
	private String country;
	private String city;
	private int plays;
	private int alive;
	private int bitrate;
	private int bufferTime;
	private String ipAddress;

	public StatsEvent(String line , SerializableIP2LocationReader reader) {
		this(null, 0, null, null, null, 0, 0, 0, 0);
		Matcher m = apacheLogRegex.matcher(line);
		
        if (m.find()) {
            ipAddress = m.group(1);
           
            try {
            	Ip2LocationRecord ipRecord =  reader.getAll(ipAddress);
            	country = ipRecord.getCountryLong();
            	city = ipRecord.getCity();
            	
            	
            	
            } catch (Exception e) {
            	e.printStackTrace();
            	country = "N/A";
            	city = "N/A";
            }
           
            if (country == null) {
            	country = "N/A";
            }
            if (city == null) {
            	city = "N/A";
            }


            
            String date = m.group(4);
            String query = m.group(5);
            
            eventTime = roundDate(date);
            
            Map<String, String> paramsMap = splitQuery(query);
            entryId = paramsMap.containsKey("event:entryId") ? paramsMap.get("event:entryId") : null;
            partnerId = Integer.parseInt(paramsMap.containsKey("event:partnerId") ? paramsMap.get("event:partnerId") : null);
            bufferTime = Integer.parseInt(paramsMap.containsKey("event:bufferTime") ? paramsMap.get("event:bufferTime") : "0");
            bitrate = Integer.parseInt(paramsMap.containsKey("event:bitrate") ? paramsMap.get("event:bitrate") : "0");
            int eventIndex = Integer.parseInt(paramsMap.containsKey("event:index") ? paramsMap.get("event:index") : "0");
            plays = eventIndex == 1 ? 1 : 0;
            alive = eventIndex > 1 ? 1 : 0;
        }
        

	}
	
	public Date roundDate(String eventDate) {
		  SimpleDateFormat formatDate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
			
			try {
				Date date = formatDate.parse(eventDate);
				Calendar c = new GregorianCalendar();
				c.setTime(date);
				int seconds = c.get(Calendar.SECOND);
				int decSeconds = seconds / 10 * 10;
				c.set(Calendar.SECOND, decSeconds);
				return c.getTime();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		
	}
	
	public Date roundHourDate(Date eventDate) {
		Calendar c = new GregorianCalendar();
		c.setTime(eventDate);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MINUTE, 0);
		return c.getTime();
			
	}
	
	public Map<String, String> splitQuery(String query)  {
	    Map<String, String> query_pairs = new LinkedHashMap<String, String>();
	    
	    String[] pairs = query.split("&");
	    for (String pair : pairs) {
	        int idx = pair.indexOf("=");
	        try {
				query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    return query_pairs;
	}
	
	
	public StatsEvent(Date eventTime, int partnerId, String entryId, String country, String city,  int plays, int alive, int bitrate, int bufferTime) {
		this.eventTime = eventTime;
		this.partnerId = partnerId;
		this.entryId = entryId;
		this.country = country;
		this.city = city;
		this.plays = plays;
	    this.alive = alive;
	    this.bitrate = bitrate;
	    this.bufferTime = bufferTime;
	}
	
	public StatsEvent merge(StatsEvent other) {
		return new StatsEvent(eventTime, partnerId, entryId, country, city, plays + other.plays, alive + other.alive, bitrate + other.bitrate, bufferTime + other.bufferTime);
	}

	public String toString() {
		return String.format("plays=%s\talive=%s\tbitrate=%s\tbuffer=%s", plays, alive, bitrate, bufferTime);
	}
	
	public Date getEventTime() {
		return this.eventTime;
	}
	
	public int getPartnerId() {
		return this.partnerId;
	}
	
	public String getEntryId() {
		return entryId;
	}
	
	public String getCountry() {
		return this.country;
	}
	
	public void setCountry(String c) {
		this.country = c;
	}
	
	public String getCity() {
		return this.city;
	}
	
	public void setCity(String city) {
		this.city = city;
	}
	
	public int getPlays() {
		return this.plays;
	}
	
	public int getAlive() {
		return this.alive;
	}
	
	public int getBitrate() {
		return this.bitrate;
	}
	
	public int getBufferTime() {
		return this.bufferTime;
	}
	
	public String getIpAddress() {
		return this.ipAddress;
	}
 }

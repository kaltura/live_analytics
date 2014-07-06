package com.kaltura.live.model;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.live.infra.cache.SerializableMemcache;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.infra.utils.RequestUtils;
import com.kaltura.ip2location.Ip2LocationRecord;
import com.kaltura.ip2location.SerializableIP2LocationReader;

/**
 *	Represents a single stats event 
 */
public class StatsEvent implements Serializable {
	
	
	private static final long serialVersionUID = 2087162345237773624L;
	
	private static Logger LOG = LoggerFactory.getLogger(StatsEvent.class);
	
	/** Regular expression representing the apache log format */
	public static Pattern apacheLogRegex = Pattern.compile(
			"^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{5})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) (\\d+\\/\\d+) \"([^\"]+)\" \"([^\"]+)\".*");
	
	/** Stats events fields */
	private Date eventTime;
	private int partnerId = 0;
	private String entryId;
	private String country;
	private String city;
	private String referrer;
	private long plays = 0;
	private long alive = 0;
	private long bitrate = 0;
	private long bitrateCount = 0;
	private long bufferTime = 0;
	private String ipAddress;
	
	
	/**
	 * Constructor by fields 
	 */
	public StatsEvent(Date eventTime, int partnerId, String entryId, String country, String city,  String referrer, long plays, long alive, long bitrate, long bitrateCount, long bufferTime) {
		this.eventTime = eventTime;
		this.partnerId = partnerId;
		this.entryId = entryId;
		this.country = country;
		this.city = city;
		this.plays = plays;
	    this.alive = alive;
	    this.bitrate = bitrate;
	    this.bitrateCount = bitrateCount;
	    this.bufferTime = bufferTime;
	    this.referrer = referrer;
	}

	/**
	 * This function parses a single apache log line and creates a single stats event from it
	 * @param line
	 * @param reader
	 * @param cache
	 */
	public StatsEvent(String line , SerializableIP2LocationReader reader, SerializableMemcache cache) {
		Matcher m = apacheLogRegex.matcher(line);
		
        if (m.find()) {
            ipAddress = m.group(1);
           
            try {
            	
            	country = (String) cache.getCache().get(ipAddress);
            	if (country == null) 
            	{
            		Ip2LocationRecord ipRecord =  reader.getAll(ipAddress);
            		country = ipRecord.getCountryLong();
            		cache.getCache().set(ipAddress, 3600, country);
            	}
            	
            } catch (Exception e) {
            	LOG.error("Failed to parse IP", e);
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
            
            eventTime = DateUtils.roundDate(date);
            
            Map<String, String> paramsMap = RequestUtils.splitQuery(query);
            entryId = paramsMap.containsKey("event:entryId") ? paramsMap.get("event:entryId") : null;
            partnerId = Integer.parseInt(paramsMap.containsKey("event:partnerId") ? paramsMap.get("event:partnerId") : null);
            bufferTime = Long.parseLong(paramsMap.containsKey("event:bufferTime") ? paramsMap.get("event:bufferTime") : "0");
            bitrate = Long.parseLong(paramsMap.containsKey("event:bitrate") ? paramsMap.get("event:bitrate") : "-1");
            referrer = paramsMap.containsKey("event:referrer") ? paramsMap.get("event:referrer") : null; 
            bitrateCount = 1;
            if (bitrate < 0)
            {
            	bitrate = 0;
            	bitrateCount = 0;
            }
            int eventIndex = Integer.parseInt(paramsMap.containsKey("event:index") ? paramsMap.get("event:index") : "0");
            plays = eventIndex == 1 ? 1 : 0;
            alive = eventIndex > 1 ? 1 : 0;
        }
        
	}
	
	
	/**
	 * Merges two stats events into a single one
	 * @param other The other stats events we'd like to merge with
	 * @return The merged stats events
	 */
	public StatsEvent merge(StatsEvent other) {
		return new StatsEvent(eventTime, partnerId, entryId, country, city, referrer, plays + other.plays, alive + other.alive, bitrate + other.bitrate, bitrateCount + other.bitrateCount, bufferTime + other.bufferTime);
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
	
	public String getReferrer()
	{
		return this.referrer;
	}
	
	public long getPlays() {
		return this.plays;
	}
	
	public long getAlive() {
		return this.alive;
	}
	
	public long getBitrate() {
		return this.bitrate;
	}
	
	public long getBitrateCount() {
		return this.bitrateCount;
	}
	
	public long getBufferTime() {
		return this.bufferTime;
	}
	
	public String getIpAddress() {
		return this.ipAddress;
	}
	
	public void setEventTime(Date eventTime) {
		this.eventTime = eventTime;
	}

	public void setPartnerId(int partnerId) {
		this.partnerId = partnerId;
	}

	public void setEntryId(String entryId) {
		this.entryId = entryId;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}

	public void setPlays(long plays) {
		this.plays = plays;
	}

	public void setAlive(long alive) {
		this.alive = alive;
	}

	public void setBitrate(long bitrate) {
		this.bitrate = bitrate;
	}

	public void setBitrateCount(long bitrateCount) {
		this.bitrateCount = bitrateCount;
	}

	public void setBufferTime(long bufferTime) {
		this.bufferTime = bufferTime;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}


	@Override
	public String toString() {
		return "StatsEvent [eventTime=" + eventTime + ", partnerId="
				+ partnerId + ", entryId=" + entryId + ", country=" + country
				+ ", city=" + city + ", referrer=" + referrer + ", plays="
				+ plays + ", alive=" + alive + ", bitrate=" + bitrate
				+ ", bitrateCount=" + bitrateCount + ", bufferTime="
				+ bufferTime + ", ipAddress=" + ipAddress + "]";
	}
	
	
 }

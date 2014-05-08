package com.kaltura.live;

import java.util.Date;

public class EntryLocationKey extends EventKey {
	
	private String entryId;
	private Date eventTime;
	private int partnerId;
	private String country;
	private String city;
	
	public EntryLocationKey(String entryId, Date eventTime, int partnerId, String country, String city) {
		this.entryId = entryId;
		this.eventTime = eventTime;
		this.partnerId = partnerId;
		this.country= country;
		this.city = city;
	}
	
	public String getEntryId() {
		return this.entryId;
	}
	
	public Date getEventTime() {
		return this.eventTime;
	}
	
	public int getPartnerId() {
		return this.partnerId;
	}
	
	public String getCountry() {
		return this.country;
	}
	
	public String getCity() {
		return this.city;
	}

	@Override
	public int hashCode() {
		 int result = 0;
	       result = 31*result + (entryId !=null ? entryId.hashCode() : 0);
	       result = 31*result + partnerId;
	       result = 31*result + (eventTime  !=null ? eventTime.hashCode() : 0);
	       result = 31*result + (country  !=null ? country.hashCode() : 0);
	       result = 31*result + (city  !=null ? city.hashCode() : 0);
	      
	       return result;
	}

	@Override
	public boolean equals(Object obj) {
		   if (this == obj)
	           return true;
	       if (obj == null)
	           return false;
	       if (getClass() != obj.getClass())
	           return false;
	       final EntryLocationKey other = (EntryLocationKey) obj;
	       if (!entryId.equals(other.getEntryId()))
	           return false;
	       if (partnerId != other.getPartnerId())
	           return false;
	       if (!eventTime.equals(other.getEventTime()))
	           return false;
	       if (!country.equals(other.getCountry()))
	           return false;
	       if (!city.equals(other.getCity()))
	           return false;
	       return true;
	}
	
	public String toString() {
		return String.format("entryId=%s\tpartnerId=%s\teventDate=%s\tcountry=%s\tcity=%s", entryId, partnerId, eventTime, country, city);
	}
}

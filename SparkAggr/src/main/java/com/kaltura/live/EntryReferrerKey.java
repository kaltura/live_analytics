package com.kaltura.live;

import java.util.Date;

public class EntryReferrerKey extends EventKey {
	
	private String entryId;
	private Date eventTime;
	private int partnerId;
	private String referrer;
	
	
	public EntryReferrerKey(String entryId, Date eventTime, int partnerId, String referrer) {
		this.entryId = entryId;
		this.eventTime = eventTime;
		this.partnerId = partnerId;
		this.referrer = referrer;
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
	
	public String getReferrer() {
		return this.referrer;
	}
	
	
	@Override
	public int hashCode() {
		 int result = 0;
	       result = 31*result + (entryId !=null ? entryId.hashCode() : 0);
	       result = 31*result + partnerId;
	       result = 31*result + (eventTime  !=null ? eventTime.hashCode() : 0);
	       result = 31*result + (referrer  !=null ? referrer.hashCode() : 0);
	       
	      
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
	       final EntryReferrerKey other = (EntryReferrerKey) obj;
	       if (!entryId.equals(other.getEntryId()))
	           return false;
	       if (partnerId != other.getPartnerId())
	           return false;
	       if (!eventTime.equals(other.getEventTime()))
	           return false;
	       if (!referrer.equals(other.getReferrer()))
	           return false;
	       
	       return true;
	}
	
	public String toString() {
		return String.format("entryId=%s\tpartnerId=%s\teventDate=%s\treferrer=%s", entryId, partnerId, eventTime, referrer);
	}
}

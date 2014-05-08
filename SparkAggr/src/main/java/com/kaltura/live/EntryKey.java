package com.kaltura.live;

import java.util.Date;

public class EntryKey extends EventKey {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7776359718630199381L;
	
	public String entryId;
	public Date eventTime;
	public int partnerId;
	
	
	public EntryKey(String entryId, Date eventTime, int partnerId) {
		this.entryId = entryId;
		this.eventTime = eventTime;
		this.partnerId = partnerId;
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

	@Override
	public int hashCode() {
		 int result = 0;
	       result = 31*result + (entryId !=null ? entryId.hashCode() : 0);
	       result = 31*result + partnerId;
	       result = 31*result + (eventTime  !=null ? eventTime.hashCode() : 0);
	      
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
	       final EntryKey other = (EntryKey) obj;
	       if (!entryId.equals(other.getEntryId()))
	           return false;
	       if (partnerId != other.getPartnerId())
	           return false;
	       if (!eventTime.equals(other.getEventTime()))
	           return false;
	       return true;
	}
	
	public String toString() {
		return String.format("entryId=%s\tpartnerId=%s\teventDate=%s", entryId, partnerId, eventTime);
	}
}

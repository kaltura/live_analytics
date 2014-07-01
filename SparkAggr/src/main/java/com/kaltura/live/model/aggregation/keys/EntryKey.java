package com.kaltura.live.model.aggregation.keys;

import java.util.Date;

public class EntryKey extends EventKey {
	
	private static final long serialVersionUID = -7776359718630199381L;
	
	protected String entryId;
	protected Date eventTime;
	protected int partnerId;
	
	public EntryKey(String entryId, Date eventTime, int partnerId) {
		this.entryId = entryId;
		this.eventTime = eventTime;
		this.partnerId = partnerId;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entryId == null) ? 0 : entryId.hashCode());
		result = prime * result
				+ ((eventTime == null) ? 0 : eventTime.hashCode());
		result = prime * result + partnerId;
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
		EntryKey other = (EntryKey) obj;
		if (entryId == null) {
			if (other.entryId != null)
				return false;
		} else if (!entryId.equals(other.entryId))
			return false;
		if (eventTime == null) {
			if (other.eventTime != null)
				return false;
		} else if (!eventTime.equals(other.eventTime))
			return false;
		if (partnerId != other.partnerId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "EntryKey [entryId=" + entryId + ", eventTime=" + eventTime
				+ ", partnerId=" + partnerId + "]";
	}

}

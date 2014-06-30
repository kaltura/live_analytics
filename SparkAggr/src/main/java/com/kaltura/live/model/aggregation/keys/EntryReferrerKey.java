package com.kaltura.live.model.aggregation.keys;

import java.util.Date;

public class EntryReferrerKey extends EventKey {
	
	private static final long serialVersionUID = 4248850425801294881L;
	
	protected String entryId;
	protected Date eventTime;
	protected int partnerId;
	protected String referrer;
	
	public EntryReferrerKey(String entryId, Date eventTime, int partnerId, String referrer) {
		this.entryId = entryId;
		this.eventTime = eventTime;
		this.partnerId = partnerId;
		this.referrer = referrer;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entryId == null) ? 0 : entryId.hashCode());
		result = prime * result
				+ ((eventTime == null) ? 0 : eventTime.hashCode());
		result = prime * result + partnerId;
		result = prime * result
				+ ((referrer == null) ? 0 : referrer.hashCode());
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
		EntryReferrerKey other = (EntryReferrerKey) obj;
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
		if (referrer == null) {
			if (other.referrer != null)
				return false;
		} else if (!referrer.equals(other.referrer))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "EntryReferrerKey [entryId=" + entryId + ", eventTime="
				+ eventTime + ", partnerId=" + partnerId + ", referrer="
				+ referrer + "]";
	}
	
}

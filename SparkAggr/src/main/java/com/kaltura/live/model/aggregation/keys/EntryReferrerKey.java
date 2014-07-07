package com.kaltura.live.model.aggregation.keys;

public class EntryReferrerKey extends EntryHourlyKey {
	
	private static final long serialVersionUID = 4248850425801294881L;
	
	protected String referrer;
	
	public EntryReferrerKey(String entryId, long eventTime, int partnerId, String referrer) {
		super(entryId, eventTime, partnerId);
		this.referrer = referrer;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entryId == null) ? 0 : entryId.hashCode());
		result = prime * result + Long.valueOf(eventTime).hashCode();
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
		if (eventTime != other.eventTime)
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

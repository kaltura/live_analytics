package com.kaltura.live.model.aggregation.keys;

import java.util.Date;

public class EntryReferrerKey extends EntryHourlyKey {
	
	private static final long serialVersionUID = 4248850425801294881L;
	
	protected String referrer;
	
	public EntryReferrerKey(String entryId, Date eventTime, int partnerId, String referrer) {
		super(entryId, eventTime, partnerId);
		this.referrer = referrer;
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((referrer == null) ? 0 : referrer.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		EntryReferrerKey other = (EntryReferrerKey) obj;
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

package com.kaltura.live.model.aggregation.keys;

import java.util.Date;

public class EntryKey extends EventKey {
	
	private static final long serialVersionUID = -7776359718630199381L;
	
	protected String entryId;
	protected int partnerId;
	
	public EntryKey(String entryId, Date eventTime, int partnerId) {
		super(eventTime);
		this.entryId = entryId;
		this.partnerId = partnerId;
	}
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((entryId == null) ? 0 : entryId.hashCode());
		result = prime * result + partnerId;
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
		EntryKey other = (EntryKey) obj;
		if (entryId == null) {
			if (other.entryId != null)
				return false;
		} else if (!entryId.equals(other.entryId))
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
	
	public String getEntryId() {
		return entryId;
	}
	
	public int getPartnerId() {
		return partnerId;
	}

}

package com.kaltura.live.model.aggregation.keys;

import java.util.Date;

public class PartnerKey extends EventKey {
	
	private static final long serialVersionUID = 5360798353836806812L;
	
	protected Date eventTime;
	protected int partnerId;
	
	public PartnerKey(int partnerId, Date eventTime) {
		this.eventTime = eventTime;
		this.partnerId = partnerId;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		PartnerKey other = (PartnerKey) obj;
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
		return "PartnerKey [eventTime=" + eventTime + ", partnerId="
				+ partnerId + "]";
	}

}

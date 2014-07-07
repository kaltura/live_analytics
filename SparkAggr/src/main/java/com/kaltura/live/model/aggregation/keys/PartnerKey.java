package com.kaltura.live.model.aggregation.keys;

public class PartnerKey extends EventKey {
	
	private static final long serialVersionUID = 5360798353836806812L;

	protected int partnerId;
	
	public PartnerKey(int partnerId, long eventTime) {
		super(eventTime);
		this.partnerId = partnerId;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Long.valueOf(eventTime).hashCode();
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
		if (eventTime != other.eventTime)
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

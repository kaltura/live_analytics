package com.kaltura.live.model.aggregation.keys;

import java.util.Date;

public class EntryLocationKey extends EntryKey {
	
	private static final long serialVersionUID = 7416244826202491902L;
	
	
	protected String country;
	protected String city;
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((city == null) ? 0 : city.hashCode());
		result = prime * result + ((country == null) ? 0 : country.hashCode());
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
		EntryLocationKey other = (EntryLocationKey) obj;
		if (city == null) {
			if (other.city != null)
				return false;
		} else if (!city.equals(other.city))
			return false;
		if (country == null) {
			if (other.country != null)
				return false;
		} else if (!country.equals(other.country))
			return false;
		return true;
	}



	public EntryLocationKey(String entryId, Date eventTime, int partnerId, String country, String city) {
		super(entryId, eventTime, partnerId);
		this.country= country;
		this.city = city;
	}

	

	@Override
	public String toString() {
		return "EntryLocationKey [entryId=" + entryId + ", eventTime="
				+ eventTime + ", partnerId=" + partnerId + ", country="
				+ country + ", city=" + city + "]";
	}
	
}

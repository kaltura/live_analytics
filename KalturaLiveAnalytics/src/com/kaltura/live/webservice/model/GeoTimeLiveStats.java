package com.kaltura.live.webservice.model;

import com.kaltura.ip2location.Coordinate;

public class GeoTimeLiveStats extends EntryLiveStats {

	protected Coordinate country;
	protected Coordinate city;
	
	public GeoTimeLiveStats() {
		super();
	}
	
	public GeoTimeLiveStats(long plays, long audience, long secondsViewed,
			long bufferTime, float avgBitrate, long timestamp, long startEvent,
			String entryId, Coordinate city, Coordinate country) {
		super(plays, audience, secondsViewed, bufferTime, avgBitrate,
				timestamp, startEvent, entryId);
		this.country = country;
		this.city = city;
	}
	
	public Coordinate getCountry() {
		return country;
	}
	public void setCountry(Coordinate country) {
		this.country = country;
	}
	public Coordinate getCity() {
		return city;
	}
	public void setCity(Coordinate city) {
		this.city = city;
	}

}

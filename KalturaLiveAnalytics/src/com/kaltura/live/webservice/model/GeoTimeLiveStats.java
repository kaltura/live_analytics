package com.kaltura.live.webservice.model;

public class GeoTimeLiveStats extends EntryLiveStats {

	protected Coordinates country;
	protected Coordinates city;
	
	public GeoTimeLiveStats() {
		super();
	}
	
	public GeoTimeLiveStats(long plays, long audience, long secondsViewed,
			long bufferTime, float avgBitrate, long timestamp, long startEvent,
			String entryId, Coordinates country, Coordinates city) {
		super(plays, audience, secondsViewed, bufferTime, avgBitrate,
				timestamp, startEvent, entryId);
		this.country = country;
		this.city = city;
	}
	
	public Coordinates getCountry() {
		return country;
	}
	public void setCountry(Coordinates country) {
		this.country = country;
	}
	public Coordinates getCity() {
		return city;
	}
	public void setCity(Coordinates city) {
		this.city = city;
	}

}

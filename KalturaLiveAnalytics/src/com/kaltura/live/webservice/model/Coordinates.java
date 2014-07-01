package com.kaltura.live.webservice.model;

public class Coordinates {
	
	protected String name;
	protected long latitude;
	protected long longtitude;
	
	
	
	public Coordinates(String name, long latitude, long longtitude) {
		super();
		this.name = name;
		this.latitude = latitude;
		this.longtitude = longtitude;
	}
	
	public long getLatitude() {
		return latitude;
	}
	public void setLatitude(long latitude) {
		this.latitude = latitude;
	}
	public long getLongtitude() {
		return longtitude;
	}
	public void setLongtitude(long longtitude) {
		this.longtitude = longtitude;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	

	@Override
	public String toString() {
		return "KalturaCoordinates [name=" + name + ", latitude=" + latitude
				+ ", longtitude=" + longtitude + "]";
	}
}

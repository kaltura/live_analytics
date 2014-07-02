package com.kaltura.live.webservice.model;


public class LiveStatsListResponse {
	
	protected LiveStats[] events;
	protected int totalCount;
	
	public LiveStatsListResponse() {
		super();
	}
	
	public LiveStatsListResponse(LiveStats[] events,
			int totalCount) {
		super();
		this.events = events;
		this.totalCount = totalCount;
	}
	
	public LiveStats[] getEvents() {
		return events;
	}
	public void setEvents(LiveStats[] events) {
		this.events = events;
	}
	public int getTotalCount() {
		return totalCount;
	}
	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}
	
	

}

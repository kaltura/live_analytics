package com.kaltura.live.webservice.model;

import java.util.Collection;

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
	
	public LiveStatsListResponse(Collection<LiveStats> events) {
		super();
		this.events = events.toArray(new EntryLiveStats[events.size()]);
		this.totalCount = events.size();
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

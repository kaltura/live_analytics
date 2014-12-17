package com.kaltura.live.webservice.model;

import java.util.Collection;

public class LiveStatsListResponse {
	
	protected LiveStats[] objects;
	protected int totalCount;
	
	public LiveStatsListResponse() {
		super();
	}
	
	public LiveStatsListResponse(LiveStats[] events,
			int totalCount) {
		super();
		this.objects = events;
		this.totalCount = totalCount;
	}
	
	public LiveStatsListResponse(Collection<LiveStats> events) {
		super();
		this.objects = events.toArray(new LiveStats[events.size()]);
		this.totalCount = events.size();
	}
	
	public LiveStatsListResponse(Collection<LiveStats> events, int count) {
		super();
		this.objects = events.toArray(new LiveStats[events.size()]);
		this.totalCount = count;
	}
	
	public LiveStats[] getObjects() {
		return objects;
	}
	public void setObjects(LiveStats[] events) {
		this.objects = events;
	}
	public int getTotalCount() {
		return totalCount;
	}
	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}
	
	

}

package com.kaltura.live.webservice.model;

import java.util.List;

public class LiveEventsListResponse {
	
	protected LiveEvent[] objects;
	protected int totalCount;
	
	public LiveEventsListResponse() {
		super();
	}
	
	public LiveEventsListResponse(List<LiveEvent> events) {
		this.totalCount = events.size();
		this.objects = events.toArray(new LiveEvent[totalCount]);
	}

	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

	public LiveEvent[] getObjects() {
		return objects;
	}

	public void setObjects(LiveEvent[] objects) {
		this.objects = objects;
	}

}

package com.kaltura.live.webservice.model;


public class LiveEventsListResponse {
	
	protected String objects;
	protected int totalCount;
	
	public LiveEventsListResponse() {
		super();
	}
	
	public LiveEventsListResponse(int size, String events) {
		this.totalCount = size;
		this.objects = events;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

	public String getObjects() {
		return objects;
	}

	public void setObjects(String objects) {
		this.objects = objects;
	}

}

package com.kaltura.live.webservice.model;

import java.util.List;

public class LiveEntriesListResponse {
	
	protected String[] entries;
	protected int totalCount;
	
	public LiveEntriesListResponse() {
		super();
	}
	
	public LiveEntriesListResponse(List<String> entries) {
		this.totalCount = entries.size();
		this.entries = entries.toArray(new String[totalCount]);
	}

	public String[] getEntries() {
		return entries;
	}

	public void setEntries(String[] entries) {
		this.entries = entries;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}
	
}

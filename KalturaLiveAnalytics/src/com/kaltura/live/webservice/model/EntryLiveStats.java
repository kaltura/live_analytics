package com.kaltura.live.webservice.model;

public class EntryLiveStats extends LiveStats {
	
	protected String entryId;
	
	public EntryLiveStats() {
		super();
	}

	public EntryLiveStats(long plays, long audience, long secondsViewed,
			long bufferTime, float avgBitrate, long timestamp, long startEvent,
			String entryId) {
		super(plays, audience, secondsViewed, bufferTime, avgBitrate,
				timestamp, startEvent);
		this.entryId = entryId;
	}

	public String getEntryId() {
		return entryId;
	}

	public void setEntryId(String entryId) {
		this.entryId = entryId;
	}
	
}

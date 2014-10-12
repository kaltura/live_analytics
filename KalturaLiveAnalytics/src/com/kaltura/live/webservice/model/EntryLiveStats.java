package com.kaltura.live.webservice.model;

public class EntryLiveStats extends LiveStats {
	
	protected String entryId;
	protected long peakAudience;
	
	public EntryLiveStats() {
		super();
	}

	public EntryLiveStats(long plays, long audience, long secondsViewed,
			float bufferTime, float avgBitrate, long timestamp,
			String entryId) {
		super(plays, audience, secondsViewed, bufferTime, avgBitrate,
				timestamp);
		this.entryId = entryId;
	}

	public String getEntryId() {
		return entryId;
	}

	public void setEntryId(String entryId) {
		this.entryId = entryId;
	}
	
}

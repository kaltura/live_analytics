package com.kaltura.live.webservice.model;

public class EntryReferrerLiveStats extends EntryLiveStats {
	
	protected String referrer;
	
	public EntryReferrerLiveStats() {
		super();
	}
	
	public EntryReferrerLiveStats(int plays, int audience,
			int secondsViewed, int bufferTime, float avgBitrate,
			long timestamp, long startEvent, String entryId, String referrer) {
		super(plays, audience, secondsViewed, bufferTime, avgBitrate,
				timestamp, startEvent, entryId);
		this.referrer = referrer;
	}

	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}
	
}

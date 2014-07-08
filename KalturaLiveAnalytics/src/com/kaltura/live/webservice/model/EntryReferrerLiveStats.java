package com.kaltura.live.webservice.model;

public class EntryReferrerLiveStats extends EntryLiveStats {
	
	protected String referrer;
	
	public EntryReferrerLiveStats() {
		super();
	}
	
	public EntryReferrerLiveStats(long plays, long audience,
			long secondsViewed, long bufferTime, float avgBitrate,
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

package com.kaltura.live.webservice.model;

public class EntryReferrerLiveStats extends EntryLiveStats {
	
	protected String referrer;
	
	public EntryReferrerLiveStats() {
		super();
	}
	
	public EntryReferrerLiveStats(long plays, long audience, long dvrAudience,
			long secondsViewed, long bufferTime, float avgBitrate,
			long timestamp, String entryId, String referrer) {
		super(plays, audience, dvrAudience, secondsViewed, bufferTime, avgBitrate,
				timestamp, entryId);
		this.referrer = referrer;
	}

	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}
	
}

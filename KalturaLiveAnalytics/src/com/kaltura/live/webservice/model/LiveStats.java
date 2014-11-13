package com.kaltura.live.webservice.model;

import javax.xml.bind.annotation.XmlSeeAlso;

@XmlSeeAlso({EntryLiveStats.class, EntryReferrerLiveStats.class, GeoTimeLiveStats.class})
public class LiveStats {
	
	protected long plays;
	protected long audience;
	protected long secondsViewed;
	protected float bufferTime;
	protected float avgBitrate;
	protected long timestamp;
	
	public LiveStats() {
		super();
	}
	
	public LiveStats(long plays, long audience, long secondsViewed,
			float bufferTime, float avgBitrate, long timestamp) {
		super();
		this.plays = plays;
		this.audience = audience;
		this.secondsViewed = secondsViewed;
		this.bufferTime = bufferTime;
		this.avgBitrate = avgBitrate;
		this.timestamp = timestamp;
	}

	public long getPlays() {
		return plays;
	}



	public void setPlays(long plays) {
		this.plays = plays;
	}



	public long getAudience() {
		return audience;
	}



	public void setAudience(long audience) {
		this.audience = audience;
	}



	public long getSecondsViewed() {
		return secondsViewed;
	}



	public void setSecondsViewed(long secondsViewed) {
		this.secondsViewed = secondsViewed;
	}



	public float getBufferTime() {
		return bufferTime;
	}



	public void setBufferTime(float bufferTime) {
		this.bufferTime = bufferTime;
	}



	public float getAvgBitrate() {
		return avgBitrate;
	}



	public void setAvgBitrate(float avgBitrate) {
		this.avgBitrate = avgBitrate;
	}



	public long getTimestamp() {
		return timestamp;
	}



	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "KalturaLiveStats [plays=" + plays + ", audience=" + audience
				+ ", secondsViewed=" + secondsViewed + ", bufferTime="
				+ bufferTime + ", avgBitrate=" + avgBitrate + ", timestamp="
				+ timestamp + "]";
	}
	
	
}

package com.kaltura.live.webservice.model;

public class LiveStats {
	
	protected int plays;
	protected int audience;
	protected int secondsViewed;
	protected int bufferTime;
	protected float avgBitrate;
	protected long timestamp;
	protected long startEvent;
	
	public LiveStats() {
		super();
	}
	
	public LiveStats(int plays, int audience, int secondsViewed,
			int bufferTime, float avgBitrate, long timestamp, long startEvent) {
		super();
		this.plays = plays;
		this.audience = audience;
		this.secondsViewed = secondsViewed;
		this.bufferTime = bufferTime;
		this.avgBitrate = avgBitrate;
		this.timestamp = timestamp;
		this.startEvent = startEvent;
	}

	public int getPlays() {
		return plays;
	}



	public void setPlays(int plays) {
		this.plays = plays;
	}



	public int getAudience() {
		return audience;
	}



	public void setAudience(int audience) {
		this.audience = audience;
	}



	public int getSecondsViewed() {
		return secondsViewed;
	}



	public void setSecondsViewed(int secondsViewed) {
		this.secondsViewed = secondsViewed;
	}



	public int getBufferTime() {
		return bufferTime;
	}



	public void setBufferTime(int bufferTime) {
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



	public long getStartEvent() {
		return startEvent;
	}



	public void setStartEvent(long startEvent) {
		this.startEvent = startEvent;
	}



	@Override
	public String toString() {
		return "KalturaLiveStats [plays=" + plays + ", audience=" + audience
				+ ", secondsViewed=" + secondsViewed + ", bufferTime="
				+ bufferTime + ", avgBitrate=" + avgBitrate + ", timestamp="
				+ timestamp + ", startEvent=" + startEvent + "]";
	}
	
	
}

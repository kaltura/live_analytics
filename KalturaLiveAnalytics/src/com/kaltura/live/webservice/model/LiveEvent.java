package com.kaltura.live.webservice.model;

public class LiveEvent {
	
	protected long value;
	protected long timestamp;
	
	public LiveEvent() {
		super();
	}

	public LiveEvent(long value, long timestamp) {
		super();
		this.value = value;
		this.timestamp = timestamp;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}

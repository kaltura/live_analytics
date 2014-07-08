package com.kaltura.live.webservice.model;

public class LiveReportInputFilter {
	
	protected long partnerId;
	protected int hoursBefore;
	protected boolean isLive;
	protected String entryIds;
	protected long eventTime;
	protected long fromTime;
	protected long toTime;
	protected String orderBy;
	
	public LiveReportInputFilter() {
		super();
	}
	
	public int getHoursBefore() {
		return hoursBefore;
	}
	public void setHoursBefore(int hoursBefore) {
		this.hoursBefore = hoursBefore;
	}
	public boolean isLive() {
		return isLive;
	}
	public void setLive(boolean isLive) {
		this.isLive = isLive;
	}
	public String getEntryIds() {
		return entryIds;
	}
	public void setEntryIds(String entryIds) {
		this.entryIds = entryIds;
	}
	public long getFromTime() {
		return fromTime;
	}
	public void setFromTime(long fromTime) {
		this.fromTime = fromTime;
	}
	public long getToTime() {
		return toTime;
	}
	public void setToTime(long toTime) {
		this.toTime = toTime;
	}
	public String getOrderBy() {
		return orderBy;
	}
	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
	}
	
	public long getPartnerId() {
		return partnerId;
	}

	public void setPartnerId(long partnerId) {
		this.partnerId = partnerId;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}

	public void validate() {
		// TODO write validator
	}

	
}

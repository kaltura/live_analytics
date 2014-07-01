package com.kaltura.live.webservice.model;

public class LiveReportInputFilter {
	
	protected int hoursBefore;
	protected boolean isLive;
	protected String entryIds;
	protected long fromTime;
	protected long toTime;
	protected String orderBy;
	
	public LiveReportInputFilter() {
		super();
	}
	
	public LiveReportInputFilter(int hoursBefore, boolean isLive,
			String entryIds, long fromTime, long toTime, String orderBy) {
		super();
		this.hoursBefore = hoursBefore;
		this.isLive = isLive;
		this.entryIds = entryIds;
		this.fromTime = fromTime;
		this.toTime = toTime;
		this.orderBy = orderBy;
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
	
	@Override
	public String toString() {
		return "KalturaLiveReportsInputFilter [hoursBefore=" + hoursBefore
				+ ", isLive=" + isLive + ", entryIds=" + entryIds
				+ ", fromTime=" + fromTime + ", toTime=" + toTime
				+ ", orderBy=" + orderBy + "]";
	}

	
}

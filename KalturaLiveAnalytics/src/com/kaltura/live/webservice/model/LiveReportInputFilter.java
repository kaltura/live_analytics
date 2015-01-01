package com.kaltura.live.webservice.model;


public class LiveReportInputFilter {
	
	protected String entryIds;
	protected long partnerId;
	
	protected boolean isLive;
	
	protected long fromTime;
	protected long toTime;
	
	protected LiveReportOrderBy orderByType;
	
	public LiveReportInputFilter() {
		super();
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
	
	public long getPartnerId() {
		return partnerId;
	}

	public void setPartnerId(long partnerId) {
		this.partnerId = partnerId;
	}
	
	public int getOrderBy() {
		return orderByType.getValue();
	}
	
	public void setOrderBy(int orderBy) {
		this.orderByType = LiveReportOrderBy.getByValue(orderBy);
	}
	
	public LiveReportOrderBy getOrderByType() {
		return orderByType;
	}

	public void validate() throws AnalyticsException {
		 if((entryIds != null) && (!entryIds.matches("^[\\w_, ]*"))) {
			 throw new AnalyticsException("Entry ids contains illegal string request.");
		 }
	}
	
	@Override
	public String toString() {
		return "LiveReportInputFilter [entryIds=" + entryIds + ", partnerId="
				+ partnerId + ", isLive=" + isLive + ", fromTime=" + fromTime
				+ ", toTime=" + toTime + "]";
	}


}

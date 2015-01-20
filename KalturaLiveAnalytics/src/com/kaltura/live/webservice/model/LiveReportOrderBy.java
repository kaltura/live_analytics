package com.kaltura.live.webservice.model;

public enum LiveReportOrderBy {

	EVENT_TIME_DESC("-eventTime"), 
	PLAYS_DESC("-plays"),
	AUDIENCE_DESC("-audience");
	
	
	protected final String value;
	
	private LiveReportOrderBy(String value) {
		this.value = value;
	}
	
	public String getValue() {
		return value;
	}
	
	public static LiveReportOrderBy getByValue(String value) {
		for (LiveReportOrderBy orderBy : LiveReportOrderBy.values()) {
			if(orderBy.getValue().equals(value))
				return orderBy;
		}
		return EVENT_TIME_DESC;
	}
	
}

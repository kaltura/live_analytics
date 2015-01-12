package com.kaltura.live.webservice.model;

public enum LiveReportOrderBy {

	EVENT_TIME_DESC("-eventTime"), 
	PLAYS_DESC("-plays");
	
	protected final String value;
	
	private LiveReportOrderBy(String value) {
		this.value = value;
	}
	
	public String getValue() {
		return value;
	}
	
	public static LiveReportOrderBy getByValue(String value) {
		for (LiveReportOrderBy orderBy : LiveReportOrderBy.values()) {
			if(value.equals(orderBy.getValue()))
				return orderBy;
		}
		return null;
	}
	
}

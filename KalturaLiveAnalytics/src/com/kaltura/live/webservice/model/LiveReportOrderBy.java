package com.kaltura.live.webservice.model;

public enum LiveReportOrderBy {

	EVENT_TIME_DESC(0), PLAYS_DESC(1);
	
	protected final int value;
	
	private LiveReportOrderBy(int value) {
		this.value = value;
	}
	
	public int getValue() {
		return value;
	}
	
	public static LiveReportOrderBy getByValue(int value) {
		for (LiveReportOrderBy orderBy : LiveReportOrderBy.values()) {
			if(orderBy.getValue() == value)
				return orderBy;
		}
		return null;
	}
	
}

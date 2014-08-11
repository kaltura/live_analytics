package com.kaltura.live.webservice.model;

public class LiveReportPager {
	
	protected int pageSize;
	protected int pageIndex;
	
	public LiveReportPager() {
		super();
	}
	
	public LiveReportPager(int pageSize, int pageIndex) {
		super();
		this.pageSize = pageSize;
		this.pageIndex = pageIndex;
	}

	public int getPageSize() {
		return pageSize;
	}
	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}
	public int getPageIndex() {
		return pageIndex;
	}
	public void setPageIndex(int pageIndex) {
		this.pageIndex = pageIndex;
	}
	
	

}

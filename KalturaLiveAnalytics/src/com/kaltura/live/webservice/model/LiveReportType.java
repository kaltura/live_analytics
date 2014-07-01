package com.kaltura.live.webservice.model;


public enum LiveReportType {

	// returns total data in the last X hours
	PARTNER_TOTAL,
	// returns entry's total data in the last X hours
	ENTRY_TOTAL,
	// returns entry's data in 10 seconds units in specific time range
	ENTRY_TIME_LINE,
	// returns live data by geographic distribution for specific entry/s in 10 seconds units
	ENTRY_GEO_TIME_LINE,
	// returns total live data by referrer
	ENTRY_SYNDICATION_TOTAL;
	
}

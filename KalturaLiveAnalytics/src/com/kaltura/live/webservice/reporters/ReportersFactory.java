package com.kaltura.live.webservice.reporters;

import com.kaltura.live.webservice.model.LiveReportType;

/**
 *	Factory for reporters
 */
final public class ReportersFactory {

	/**
	 * Returns the right statistics manager by required type.
	 * @param type The required report type
	 * @return The matching statistics manager
	 */
	public static final BaseReporter getReporter(LiveReportType type) {
		switch (type) {
		case ENTRY_TOTAL:
			return new EntryTotalReporter();
		case ENTRY_GEO_TIME_LINE:
			return new EntryGeoTimeLineReporter();
		case ENTRY_SYNDICATION_TOTAL:
			return new EntrySyndicationTotalReporter();
		case ENTRY_TIME_LINE:
			return new EntryTimeLineReporter();
		case PARTNER_TOTAL:
			return new PartnerTotalReporter();
			
		default:
			throw new RuntimeException("Live report type [" + type + "] isn't supported");
		}
	}
}

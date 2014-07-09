package com.kaltura.live.webservice.reporters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.model.aggregation.dao.LiveEntryEventDAO;
import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.EntryLiveStats;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

public class EntryTimeLineReporter extends BaseReporter {
	
	@Override
	public LiveStatsListResponse query(LiveReportInputFilter filter) {
		String query = generateQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Iterator<Row> itr = results.iterator();
		
		List<LiveStats> result = new ArrayList<LiveStats>();
		while(itr.hasNext()) {
			LiveEntryEventDAO dao = new LiveEntryEventDAO(itr.next());
			float avgBitrate = 0;
			if(dao.getBitrateCount() > 0)
				avgBitrate = dao.getBitrate() / dao.getBitrateCount();
			EntryLiveStats event = new EntryLiveStats(dao.getPlays(), dao.getAlive(), dao.getAlive() * 10, 
					dao.getBufferTime(), avgBitrate, (long)0, (long)0, dao.getEntryId());
			result.add(event);
		}
		
		return new LiveStatsListResponse(result);
	}

	private String generateQuery(LiveReportInputFilter filter) {
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.live_events where ");
		sb.append(addEntryIdsCondition(filter.getEntryIds()));
		sb.append(" and ");
		sb.append(addTimeRangeCondition(
				DateUtils.roundDate(filter.getFromTime() * 1000), 
				DateUtils.roundDate(filter.getToTime() * 1000)));
		sb.append(";");
		
		String query = sb.toString();
		logger.debug(query);
		return query;
	}
	
	@Override
	public void validateFilter(LiveReportInputFilter filter) throws AnalyticsException {
		
		String validation = "";
		if(filter.getEntryIds() == null)
			validation = "Entry Ids can't be null. ";
		if(filter.getFromTime() < 0)
			validation += "From Time must be a positive number.";
		if(filter.getToTime() < 0)
			validation += "To Time must be a positive number.";
		
		if(!validation.isEmpty())
			throw new AnalyticsException("Illegal filter input: " + validation);
	}
	
}

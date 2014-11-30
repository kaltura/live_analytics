package com.kaltura.live.webservice.reporters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.model.aggregation.dao.LiveEntryEventDAO;
import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.LiveEventsListResponse;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportPager;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

public class EntryTimeLineReporter extends BaseReporter {
	
	public LiveEventsListResponse eventsQuery(LiveReportInputFilter filter, LiveReportPager pager) {
		String query = generateQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Iterator<Row> itr = results.iterator();
		List<String> result = new ArrayList<String>();
		while(itr.hasNext()) {
			LiveEntryEventDAO dao = new LiveEntryEventDAO(itr.next());
			result.add((dao.getEventTime().getTime() / 1000) + "," + dao.getAlive());
		}
		
		StringBuffer sb = new StringBuffer();
		for (int i = result.size() - 1; i >= 0 ; --i) {
			sb.append(result.get(i));
			sb.append(";");
		}
		
		return new LiveEventsListResponse(result.size(), sb.toString());
	}

	private String generateQuery(LiveReportInputFilter filter) {
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.live_events where ");
		sb.append(addEntryIdsCondition(filter.getEntryIds()));
		sb.append(" and ");
		sb.append(addTimeRangeCondition(
				DateUtils.roundDate(filter.getFromTime()), 
				DateUtils.roundDate(filter.getToTime())));
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
		if(!isValidateEntryIds(filter.getEntryIds())) 
			validation += "Entry Ids contain illegal characters. ";
		if(filter.getFromTime() < 0)
			validation += "From Time must be a positive number.";
		if(filter.getToTime() < 0)
			validation += "To Time must be a positive number.";
		
		if(!validation.isEmpty())
			throw new AnalyticsException("Illegal filter input: " + validation);
	}

	@Override
	public LiveStatsListResponse query(LiveReportInputFilter filter, LiveReportPager pager) {
		return null;
	}

}

package com.kaltura.live.webservice.reporters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.model.aggregation.dao.LiveEntryEventDAO;
import com.kaltura.live.model.aggregation.dao.PartnerEventDAO;
import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportPager;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

public class PartnerTotalReporter extends BaseReporter {
	
	@Override
	public LiveStatsListResponse query(LiveReportInputFilter filter, LiveReportPager pager) {

		if(filter.isLive())
			return generateLiveReport(filter);
		else
			return generatePastReport(filter);
	}
	
	protected String generateLiveEntriesQuery(LiveReportInputFilter filter) {
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.live_events where ");
		sb.append(addEntryIdsCondition(filter.getEntryIds()));
		sb.append(" and ");
		sb.append(addTimeRangeCondition(DateUtils.roundDate(filter.getFromTime()), DateUtils.roundDate(filter.getToTime())));
		sb.append(";");
		
		String query = sb.toString();
		logger.debug(query);
		return query;
	}
	
	private LiveStatsListResponse generateLiveReport(
			LiveReportInputFilter filter) {
		String query = generateLiveEntriesQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Iterator<Row> itr = results.iterator();
		ReportsAggregator aggr = new ReportsAggregator();
		while(itr.hasNext()) {
			LiveEntryEventDAO dao = new LiveEntryEventDAO(itr.next());
			aggr.aggregateResult(dao.getPlays(), dao.getAlive(), dao.getDVRAlive(), dao.getBufferTime(), dao.getBitrate(), dao.getBitrateCount());
		}
		
		LiveStats entry = new LiveStats();
		aggr.fillObject(entry);
		
		List<LiveStats> result = new ArrayList<LiveStats>();
		result.add(entry);
		return new LiveStatsListResponse(result);
	}

	protected String generatePastPartnerQuery(LiveReportInputFilter filter) {
		
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.hourly_live_events_partner where partner_id = ");
		sb.append(filter.getPartnerId());
		sb.append(" and ");
		sb.append(addTimeInHourRangeCondition(filter.getFromTime(),filter.getToTime()));
		sb.append(";");
		
		String query = sb.toString();
		logger.debug(query);
		return query;
	}

	private LiveStatsListResponse generatePastReport(
			LiveReportInputFilter filter) {
		String query = generatePastPartnerQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Iterator<Row> itr = results.iterator();
		Map<Integer, ReportsAggregator> map = new HashMap<Integer, ReportsAggregator>();
		while(itr.hasNext()) {
			PartnerEventDAO dao = new PartnerEventDAO(itr.next());
			int key = dao.getPartnerId();
			if(!map.containsKey(key)) {
				map.put(key, new ReportsAggregator());
			}
			map.get(key).aggregateResult(dao.getPlays(), dao.getAlive(), dao.getDVRAlive(), dao.getBufferTime(), dao.getBitrate(), dao.getBitrateCount());
		}
		
		List<LiveStats> result = new ArrayList<LiveStats>();
		for (Entry<Integer, ReportsAggregator> stat : map.entrySet()) {
			LiveStats statObj = new LiveStats();
			stat.getValue().fillObject(statObj);
			result.add(statObj);
		}
		
		return new LiveStatsListResponse(result);
	}
	
	@Override
	public void validateFilter(LiveReportInputFilter filter) throws AnalyticsException {
		
		String validation = "";
		
		if(filter.isLive()) {
			if(filter.getEntryIds() == null)
				validation = "Entry Ids can't be null. ";
			if(!isValidateEntryIds(filter.getEntryIds())) 
				validation += "Entry Ids contain illegal characters. ";
		} else {
			if(filter.getPartnerId() < 0)
				validation += "Partner Id must be a positive number.";
		}
		
		if(!validation.isEmpty())
			throw new AnalyticsException("Illegal filter input: " + validation);
	}
	
	
}

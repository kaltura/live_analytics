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
import com.kaltura.live.model.aggregation.dao.LiveEntryPeakDAO;
import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.EntryLiveStats;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportPager;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

public class EntryTotalReporter extends BaseReporter {
	
	@Override
	public LiveStatsListResponse query(LiveReportInputFilter filter, LiveReportPager pager) {

		if(filter.isLive())
			return generateLiveReport(filter);
		else
			return generatePastEntryReport(filter);
	}
	
	/** ------------- Handle Recorded entry --------------*/ 
	
	protected String generatePastEntryQuery(LiveReportInputFilter filter) {
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.hourly_live_events where ");
		sb.append(addEntryIdsCondition(filter.getEntryIds()));
		sb.append(" and ");
		sb.append(addTimeRangeCondition(DateUtils.roundDate(filter.getFromTime()), DateUtils.roundDate(filter.getToTime())));
		sb.append(";");
		
		String query = sb.toString();
		logger.debug(query);
		return query;
	}
	
	protected LiveStatsListResponse generatePastEntryReport(
			LiveReportInputFilter filter) {
		String query = generatePastEntryQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Map<String, ReportsAggregator> map = new HashMap<String, ReportsAggregator>();
		Iterator<Row> itr = results.iterator();
		while(itr.hasNext()) {
			LiveEntryEventDAO dao = new LiveEntryEventDAO(itr.next());
			String key = dao.getEntryId();
			if(!map.containsKey(key)) {
				map.put(key, new ReportsAggregator());
			}
			
			map.get(key).aggregateResult(dao.getPlays(), dao.getAlive(), dao.getBufferTime(), dao.getBitrate(), dao.getBitrateCount());
		}

		List<LiveStats> result = new ArrayList<LiveStats>();
		for (Entry<String, ReportsAggregator> stat : map.entrySet()) {
			EntryLiveStats entry = new EntryLiveStats();
			entry.setEntryId(stat.getKey());
			stat.getValue().fillObject(entry);
			result.add(entry);
		}
		
		calculatePeakAudience(filter, result);
		return new LiveStatsListResponse(result);
	}
	
	protected void aggregateResult(Map<String, Long> mapping, String key, long value) {
		if(mapping.containsKey(key)) {
			value += mapping.get(key);
		}
		mapping.put(key, value);
	}

	/** ------------- Handle Live entry --------------*/
	
	protected String generateLiveQuery(LiveReportInputFilter filter) {
		
		
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
	
	protected LiveStatsListResponse generateLiveReport(
			LiveReportInputFilter filter) {
		
		String query = generateLiveQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Map<String, ReportsAggregator> map = new HashMap<String, ReportsAggregator>();
		Iterator<Row> itr = results.iterator();
		while(itr.hasNext()) {
			LiveEntryEventDAO dao = new LiveEntryEventDAO(itr.next());
			String key = dao.getEntryId();
			if(!map.containsKey(key)) {
				map.put(key, new ReportsAggregator());
			}
			
			map.get(key).aggregateResult(dao.getPlays(), dao.getAlive(), dao.getBufferTime(), dao.getBitrate(), dao.getBitrateCount());
		}
		
		List<LiveStats> result = new ArrayList<LiveStats>();
		for (Entry<String, ReportsAggregator> stat : map.entrySet()) {
			EntryLiveStats entry = new EntryLiveStats();
			entry.setEntryId(stat.getKey());
			stat.getValue().fillObject(entry);
			result.add(entry);
		}
		
		calculatePeakAudience(filter, result);
		return new LiveStatsListResponse(result);
	}
	
	protected String generatePeakAudienceQuery(LiveReportInputFilter filter) {
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.live_entry_hourly_peak where ");
		sb.append(addEntryIdsCondition(filter.getEntryIds()));
		sb.append(" and ");
		sb.append(addTimeRangeCondition(DateUtils.roundDate(filter.getFromTime()), DateUtils.roundDate(filter.getToTime())));
		sb.append(";");
		
		String query = sb.toString();
		logger.debug(query);
		return query;
	}
	
	protected void calculatePeakAudience(LiveReportInputFilter filter, List<LiveStats> liveStats) {
		String query = generatePeakAudienceQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		// Retrieve entryId-peakAudience
		Map<String, Long> peakAudience = new HashMap<String, Long>();
		Iterator<Row> itr = results.iterator();
		while(itr.hasNext()) {
			LiveEntryPeakDAO dao = new LiveEntryPeakDAO(itr.next());
			String key = dao.getEntryId();
			Long value = dao.getAudience();
			
			Long curVal = peakAudience.get(key);
			if((curVal == null) || (curVal < value))
				peakAudience.put(key, value);
		}
		
		// Fill peak audience
		for (LiveStats liveStat : liveStats) {
			EntryLiveStats stat = (EntryLiveStats)liveStat;
			String entryId = stat.getEntryId();
			Long peakAudienceVal = peakAudience.get(entryId);
			if(peakAudienceVal != null)
				stat.setPeakAudience(peakAudienceVal);
		}
	}
	
	@Override
	public void validateFilter(LiveReportInputFilter filter) throws AnalyticsException {
		
		String validation = "";
		if(filter.getEntryIds() == null)
			validation = "Entry Ids can't be null. ";
		
		if(filter.getFromTime() <= 0)
			validation += "From time must be a timestamp ";
		
		if(filter.getToTime() <= 0)
			validation += "To time must be a timestamp ";
			
		if(!validation.isEmpty())
			throw new AnalyticsException("Illegal filter input: " + validation);
	}
	
}

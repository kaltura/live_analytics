package com.kaltura.live.webservice.reporters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.model.aggregation.dao.LiveEntryEventDAO;
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
		sb.append(addHoursBeforeCondition(DateUtils.getCurrentTime().getTime(), filter.getHoursBefore()));
		sb.append(";");
		
		String query = sb.toString();
		logger.debug(query);
		System.out.println(query);
		return query;
	}
	
	protected LiveStatsListResponse generatePastEntryReport(
			LiveReportInputFilter filter) {
		String query = generatePastEntryQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Map<String, Long> plays = new HashMap<String, Long>();
		Map<String, Long> alive = new HashMap<String, Long>();
		Map<String, Long> bitrate = new HashMap<String, Long>();
		Map<String, Long> bitrateCount = new HashMap<String, Long>();
		Map<String, Long> bufferTime = new HashMap<String, Long>();
		
		Iterator<Row> itr = results.iterator();
		while(itr.hasNext()) {
			LiveEntryEventDAO dao = new LiveEntryEventDAO(itr.next());
			String key = dao.getEntryId();
			aggregateResult(plays, key, dao.getPlays());
			aggregateResult(alive, key, dao.getAlive());
			aggregateResult(bitrate, key, dao.getBitrate());
			aggregateResult(bitrateCount, key, dao.getBitrateCount());
			aggregateResult(bufferTime, key, dao.getBufferTime());
		}

		List<LiveStats> result = new ArrayList<LiveStats>();
		for (String entryId : plays.keySet()) {
			EntryLiveStats entry = new EntryLiveStats();
			entry.setEntryId(entryId);
			entry.setPlays(plays.get(entryId).longValue());
			entry.setSecondsViewed(alive.get(entryId).longValue() * 10);
			entry.setBufferTime(bufferTime.get(entryId).longValue());
			
			int bitrateCountVal = (int)bitrateCount.get(entryId).longValue();
			if(bitrateCountVal > 0)
				entry.setAvgBitrate(bitrate.get(entryId) / bitrateCountVal);
			else 
				entry.setAvgBitrate(0);
			result.add(entry);
		}
		
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
		sb.append(addNowCondition());
		sb.append(";");
		
		String query = sb.toString();
		logger.debug(query);
		System.out.println(query);
		return query;
	}
	
	protected LiveStatsListResponse generateLiveReport(
			LiveReportInputFilter filter) {
		
		String query = generateLiveQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Iterator<Row> itr = results.iterator();
		List<LiveStats> result = new ArrayList<LiveStats>();
		while(itr.hasNext()) {
			LiveEntryEventDAO dao = new LiveEntryEventDAO(itr.next());
			EntryLiveStats entry = new EntryLiveStats();
			entry.setEntryId(dao.getEntryId());
			entry.setAudience(dao.getAlive());
			entry.setSecondsViewed(dao.getAlive() * 10);
			entry.setBufferTime(dao.getBufferTime());
			
			long bitrateCountVal = dao.getBitrate();
			if(bitrateCountVal > 0)
				entry.setAvgBitrate(dao.getBitrateCount() / bitrateCountVal);
			else 
				entry.setAvgBitrate(0);
			
			result.add(entry);
		}
		
		return new LiveStatsListResponse(result);
	}
	
	@Override
	public void validateFilter(LiveReportInputFilter filter) throws AnalyticsException {
		
		String validation = "";
		if(filter.getEntryIds() == null)
			validation = "Entry Ids can't be null. ";
		
		if(!filter.isLive()) {
			if(filter.getHoursBefore() < 0)
				validation += "Hourse before must be a positive number.";
		}
		
		if(!validation.isEmpty())
			throw new AnalyticsException("Illegal filter input: " + validation);
	}
	
}

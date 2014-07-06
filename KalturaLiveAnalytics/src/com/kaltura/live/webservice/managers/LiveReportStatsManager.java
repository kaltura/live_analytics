package com.kaltura.live.webservice.managers;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.Configuration;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.webservice.model.EntryLiveStats;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

public class LiveReportStatsManager implements StatisticsManagerIfc {
	
	// TODO add logger usage.
	// TODO change to long also in api.
	// TODO extract all these gets to a DAO Object.
	// TODO add model as common project,
	// TODO same for infra
	
	private static final int TIME_FRAME_INTERVAL = 10;
	private static final int TIME_FRAME_INTERVAL_COUNT = 6;
	
	private static SerializableSession session;
	
	public LiveReportStatsManager() {
		session = new SerializableSession(Configuration.NODE_NAME);
	}

	@Override
	public LiveStatsListResponse query(LiveReportInputFilter filter) {

		if(filter.isLive())
			return generateLiveReport(filter);
		else
			return generateRecordedEntryReport(filter);
	}
	
	/** ------------- General functionality --------------*/ 
	
	protected String getTimeLineRequestString(int nIntervals) {
		SimpleDateFormat formatDate = new SimpleDateFormat(DATE_FORMAT);
		Calendar cal = DateUtils.getCurrentTime();
		StringBuffer hoursStr = new StringBuffer();
		for(int i = 0 ; i < nIntervals ; ++i) {
			Date roundTime = DateUtils.roundDate(cal.getTime());
			hoursStr.append("'" + formatDate.format(roundTime) + "'");
			// TODO - maybe use > and < instead.
			cal.add(Calendar.SECOND, -TIME_FRAME_INTERVAL);
			if (i != nIntervals - 1) 
				hoursStr.append(",");
		}
		return hoursStr.toString();
	}
	
	protected String escapeEntryIdsCondition(String entryIdsStr) {
		StringBuffer sb = new StringBuffer();
		// Add entry Ids
		String[] entryIds = entryIdsStr.split(",");
		int cnt = entryIds.length - 1;
		for (String entryId : entryIds) {
			sb.append("'" + entryId + "'");
			if(cnt-- != 0)
				sb.append(",");
		}
		return sb.toString();
	}
	
	protected String getHoursRequestString(int nHours) {
		SimpleDateFormat formatDate = new SimpleDateFormat(DATE_FORMAT);
		Calendar cal = DateUtils.getCurrentTime();
		StringBuffer hoursStr = new StringBuffer();
		
		Date roundTime = DateUtils.roundHourDate(cal.getTime());
		hoursStr.append("'" + formatDate.format(roundTime) + "'");
		
		for(int i = 0 ; i < nHours ; ++i) {
			cal.add(Calendar.HOUR, -1);
			hoursStr.append(",");
			roundTime = DateUtils.roundHourDate(cal.getTime());
			hoursStr.append("'" + formatDate.format(roundTime) + "'");
		}
		return hoursStr.toString();
	}

	/** ------------- Handle Recorded entry --------------*/ 
	
	protected String generateRecordedQuery(LiveReportInputFilter filter) {
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.hourly_live_events where entry_id IN (");
		sb.append(escapeEntryIdsCondition(filter.getEntryIds()));
		sb.append(") and event_time in (");
		sb.append(getHoursRequestString(filter.getHoursBefore()));
		sb.append(");");
		
		String query = sb.toString();
		System.out.println("@_!! " + query);
		return query;
	}
	
	protected LiveStatsListResponse generateRecordedEntryReport(
			LiveReportInputFilter filter) {
		String query = generateRecordedQuery(filter);
		
		// TODO - move to work with iterator
		// TODO - in case we'd like to use paging, we need to use paging token_entry_id_ > 
		// TODO - verify we do close the session and the cluster
		// TODO - in the connect verify we connect to the key-space
		ResultSet results = session.getSession().execute(query);
		
		Map<String, Long> plays = new HashMap<String, Long>();
		Map<String, Long> alive = new HashMap<String, Long>();
		Map<String, Long> bitrate = new HashMap<String, Long>();
		Map<String, Long> bitrateCount = new HashMap<String, Long>();
		Map<String, Long> bufferTime = new HashMap<String, Long>();
		
		for (Row result : results) {
			String key = result.getString("entry_id");
			aggregateResult(result, key, plays, "plays");
			aggregateResult(result, key, alive, "alive");
			aggregateResult(result, key, bitrate, "bitrate");
			aggregateResult(result, key, bitrateCount, "bitrate_count");
			aggregateResult(result, key, bufferTime, "buffer_time");
		}

		List<EntryLiveStats> result = new ArrayList<EntryLiveStats>();
		for (String entryId : plays.keySet()) {
			EntryLiveStats entry = new EntryLiveStats();
			entry.setEntryId(entryId);
			entry.setPlays((int)plays.get(entryId).longValue());
			entry.setSecondsViewed((int)alive.get(entryId).longValue() * 10);
			entry.setBufferTime((int)bufferTime.get(entryId).longValue());
			
			int bitrateCountVal = (int)bitrateCount.get(entryId).longValue();
			if(bitrateCountVal > 0)
				entry.setAvgBitrate(bitrate.get(entryId) / bitrateCountVal);
			else 
				entry.setAvgBitrate(0);
			result.add(entry);
		}
		
		return new LiveStatsListResponse(result.toArray(new EntryLiveStats[result.size()]), result.size());
	}
	
	protected void aggregateResult(Row result, String key, Map<String, Long> mapping, String fieldName) {
		long res = result.getLong(fieldName);
		if(mapping.containsKey(key)) {
			res += mapping.get(key);
		}
		mapping.put(key, res);
	}

	/** ------------- Handle Live entry --------------*/
	
	protected String generateLiveQuery(LiveReportInputFilter filter) {
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.live_events where entry_id IN (");
		sb.append(escapeEntryIdsCondition(filter.getEntryIds()));
		sb.append(") and event_time = ");
		// TODO - set to half minute ago
		sb.append(";");
		
		String query = sb.toString();
		System.out.println("@_!! " + query);
		return query;
	}
	
	protected LiveStatsListResponse generateLiveReport(
			LiveReportInputFilter filter) {
		
		String query = generateLiveQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Map<String, Row> mapping = new HashMap<String, Row>();
		for (Row result : results) {
			String key = result.getString("entry_id");
			if(mapping.containsKey(key)) {
				Date oldEvent = mapping.get(key).getDate("event_time");
				Date curEvent = result.getDate("event_time");
				// TODO consider replacing this hack with order by.
				if(curEvent.after(oldEvent))
					mapping.put(key, result);
			} else {
				mapping.put(key, result);
			}
		}
		
		List<EntryLiveStats> result = new ArrayList<EntryLiveStats>();
		for (Entry<String, Row> statsEvent : mapping.entrySet()) {
			Row row = statsEvent.getValue();
			EntryLiveStats entry = new EntryLiveStats();
			entry.setEntryId(statsEvent.getKey());
			entry.setAudience(row.getInt("alive"));
			entry.setSecondsViewed(row.getInt("alive") * 10);
			entry.setBufferTime(row.getInt("buffer_time"));
			
			int bitrateCountVal = row.getInt("bitrate");
			if(bitrateCountVal > 0)
				entry.setAvgBitrate(row.getInt("bitrate_count") / bitrateCountVal);
			else 
				entry.setAvgBitrate(0);
			
			result.add(entry);
		}
		
		return new LiveStatsListResponse(result.toArray(new EntryLiveStats[result.size()]), result.size());
	}
}

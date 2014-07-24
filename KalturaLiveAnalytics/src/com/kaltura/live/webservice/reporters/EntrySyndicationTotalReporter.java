package com.kaltura.live.webservice.reporters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.model.aggregation.dao.LiveEntryReferrerEventDAO;
import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.EntryReferrerLiveStats;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

public class EntrySyndicationTotalReporter extends BaseReporter {
	
	@Override
	public LiveStatsListResponse query(LiveReportInputFilter filter) {
		String query = generateQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		java.util.Iterator<Row> itr = results.iterator();
		// Sum plays per referrer
		Map<String, EntryReferrerLiveStats> map = new TreeMap<String, EntryReferrerLiveStats>();
		while (itr.hasNext()) {
			LiveEntryReferrerEventDAO dao = new LiveEntryReferrerEventDAO(itr.next());
			String key = dao.getReferrer();
			long value = dao.getPlays();
			if(map.containsKey(key)) { 
				value += map.get(key).getPlays();
				map.get(key).setPlays(value);
			} else {
				map.put(key, new EntryReferrerLiveStats(value, 0, 0, 0, 0, 0, 0, null, key));
			}
		}
		
		List<LiveStats> stats = new ArrayList<LiveStats>(map.values());
		Collections.sort(stats, new Comparator<LiveStats>() {
			@Override
			public int compare(LiveStats o1, LiveStats o2) {
				return (int) (o2.getPlays() - o1.getPlays());
			}
		});
		
		List<LiveStats> limitedRes = stats.subList(0, Math.min(stats.size(), filter.getResultsLimit()));
		
		return new LiveStatsListResponse(limitedRes);
	}
	
	protected String generateQuery(LiveReportInputFilter filter) {
		
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.hourly_live_events_referrer where ");
		sb.append(addEntryIdsCondition(filter.getEntryIds()));
		sb.append(" and ");
		sb.append(addHoursBeforeCondition(DateUtils.getCurrentTime().getTime(), filter.getHoursBefore()));
		sb.append(";");
		
		String query = sb.toString();
		logger.debug(query);
		return query;
	}

	@Override
	public void validateFilter(LiveReportInputFilter filter) throws AnalyticsException {
		
		String validation = "";
		if(filter.getEntryIds() == null)
			validation = " Entry Ids can't be null. ";
		if(filter.getHoursBefore() < 0)
			validation += " Hours before must be a positive number.";
		if(filter.getResultsLimit() <= 0)
			validation += " results limit must be a positive number.";
		
		if(!validation.isEmpty())
			throw new AnalyticsException("Illegal filter input: " + validation);
	}
}

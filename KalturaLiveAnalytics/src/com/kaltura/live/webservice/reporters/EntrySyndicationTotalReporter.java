package com.kaltura.live.webservice.reporters;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.model.aggregation.dao.LiveEntryReferrerEventDAO;
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
		
		TreeSet<LiveStats> set = new TreeSet<LiveStats>(new Comparator<LiveStats>() {

			@Override
			public int compare(LiveStats o1, LiveStats o2) {
				return (int) (o1.getPlays() - o2.getPlays());
			}
		});
		
		set.addAll(map.values());
		
		return new LiveStatsListResponse(set);
	}
	
	protected String generateQuery(LiveReportInputFilter filter) {
		
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.hourly_live_events_referrer where ");
		sb.append(addEntryIdsCondition(filter.getEntryIds()));
		sb.append(" and ");
		sb.append(addHoursBeforeCondition(DateUtils.getCurrentTime().getTime(), filter.getHoursBefore()));
		sb.append(";");
		
		String query = sb.toString();
		System.out.println("@_!! " + query);
		return query;
	}
}

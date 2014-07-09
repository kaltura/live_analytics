package com.kaltura.live.webservice.reporters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.model.aggregation.dao.LiveEntryLocationEventDAO;
import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.Coordinates;
import com.kaltura.live.webservice.model.GeoTimeLiveStats;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

public class EntryGeoTimeLineReporter extends BaseReporter {
	
	@Override
	public LiveStatsListResponse query(LiveReportInputFilter filter) {
		String query = generateQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Iterator<Row> itr = results.iterator();
		
		List<LiveStats> result = new ArrayList<LiveStats>();
		while(itr.hasNext()) {
			// TODO add country city to long - latitude.
			LiveEntryLocationEventDAO dao = new LiveEntryLocationEventDAO(itr.next());
			float avgBitrate = 0;
			if(dao.getBitrateCount() > 0)
				avgBitrate = dao.getBitrate() / dao.getBitrateCount();
			GeoTimeLiveStats event = new GeoTimeLiveStats(dao.getPlays(), dao.getAlive(), dao.getAlive() * 10, 
					dao.getBufferTime(), avgBitrate, (long)0, (long)0, dao.getEntryId(), 
					new Coordinates(dao.getCountry(), 0, 0), 
					new Coordinates(dao.getCity(), 0, 0));
			result.add(event);
		}
		
		return new LiveStatsListResponse(result);
	}

	private String generateQuery(LiveReportInputFilter filter) {
		
		long timeInMs = filter.getEventTime() * 1000;
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.live_events_location where ");
		sb.append(addEntryIdsCondition(filter.getEntryIds()));
		sb.append(" and ");
		sb.append(addExactTimeCondition(DateUtils.roundDate(timeInMs)));
		sb.append(";");
		
		String query = sb.toString();
		logger.debug(query);
		return query;
	}

	@Override
	public void validateFilter(LiveReportInputFilter filter) throws AnalyticsException {
		if(filter.getEntryIds() == null)
			throw new AnalyticsException("Illegal filter input. Entry Ids can't be null.");
	}
	
	

}

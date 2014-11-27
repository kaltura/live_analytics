package com.kaltura.live.webservice.reporters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.ip2location.Coordinate;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.model.aggregation.dao.LiveEntryLocationEventDAO;
import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.GeoTimeLiveStats;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportPager;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

public class EntryGeoTimeLineReporter extends BaseReporter {
	
	@Override
	public LiveStatsListResponse query(LiveReportInputFilter filter, LiveReportPager pager) {
		String query = generateQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Iterator<Row> itr = results.iterator();
		List<LiveStats> result = new ArrayList<LiveStats>();
		while(itr.hasNext()) {
			LiveEntryLocationEventDAO dao = new LiveEntryLocationEventDAO(itr.next());
			
			GeoTimeLiveStats res = new GeoTimeLiveStats();
			res.setEntryId(dao.getEntryId());
			
			Coordinate country = GeographicalLocatorsCache.getCountryLocator().getCountryCoordinates(dao.getCountry());
			res.setCountry(country);
			
			Coordinate city = GeographicalLocatorsCache.getCityLocator().getCityCoordinates(dao.getCountry(), dao.getCity());
			res.setCity(city);
			
			res.setTimestamp(dao.getEventTime().getTime() / 1000);
			
			ReportsAggregator aggr = new ReportsAggregator();
			aggr.aggregateResult(dao.getPlays(), dao.getAlive(), dao.getBufferTime(), dao.getBitrate(), dao.getBitrateCount());
			aggr.fillObject(res);
			
			result.add(res);
		}
		
		return new LiveStatsListResponse(result);
	}

	private String generateQuery(LiveReportInputFilter filter) {
		
		StringBuffer sb = new StringBuffer();
		sb.append("select * from kaltura_live.live_events_location where ");
		sb.append(addEntryIdsCondition(filter.getEntryIds()));
		sb.append(" and ");
		sb.append(addTimeInRangeCondition(DateUtils.roundDate(filter.getFromTime()), DateUtils.roundDate(filter.getToTime())));
		sb.append(";");
		
		String query = sb.toString();
		logger.debug(query);
		return query;
	}

	@Override
	public void validateFilter(LiveReportInputFilter filter) throws AnalyticsException {
		String validation = "";
		if(filter.getEntryIds() == null)
			validation += "Entry Ids can't be null. ";
		if(!isValidateEntryIds(filter.getEntryIds())) 
			validation += "Entry Ids contain illegal characters. ";
		if(filter.getFromTime() == 0)
			validation += "From time must be a timestamp ";
		if(filter.getToTime() == 0)
			validation += "To time must be a timestamp ";
		
		if(!validation.isEmpty())
			throw new AnalyticsException("Illegal filter input: " + validation);
		
	}
	
	

}

package com.kaltura.live.webservice.reporters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import com.kaltura.live.webservice.model.LiveReportOrderBy;
import com.kaltura.live.webservice.model.LiveReportPager;
import com.kaltura.live.webservice.model.LiveStats;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

public class EntryGeoTimeLineReporter extends BaseReporter {
	
	@Override
	public LiveStatsListResponse query(LiveReportInputFilter filter, LiveReportPager pager) {
		
		int pageIdx = (pager == null) ? 1 : pager.getPageIndex();
		int pageSize = (pager == null) ? Integer.MAX_VALUE : pager.getPageSize();
		boolean specificOrderBy = !LiveReportOrderBy.EVENT_TIME_DESC.equals(filter.getOrderByType());
		
		// Select count
		int count = getRecordsCount(filter);
		
		String query = generateQuery(filter, false);
		ResultSet results = session.getSession().execute(query);
		
		Iterator<Row> itr = results.iterator();
		List<LiveStats> result = new ArrayList<LiveStats>();
		int i = 0;
		while(itr.hasNext()) {
			Row row = itr.next();
			
			// Pager check 
			// This code was written as a result of unsuccessful use of paging with Cassandra, once you overcome this - 
			// Please remove this code
			if(!specificOrderBy) {
				if(i >= pageIdx * pageSize)
					break;
				
				boolean inRange = (i >= (pageIdx - 1) * pageSize);
				i++;
				if(!inRange)
					continue;
			}
			
			LiveEntryLocationEventDAO dao = new LiveEntryLocationEventDAO(row);
			
			GeoTimeLiveStats res = new GeoTimeLiveStats();
			res.setEntryId(dao.getEntryId());
			
			Coordinate country = GeographicalLocatorsCache.getCountryLocator().getCountryCoordinates(dao.getCountry().toUpperCase());
			res.setCountry(country);
			
			Coordinate city = GeographicalLocatorsCache.getCityLocator().getCityCoordinates(dao.getCountry().toUpperCase(), dao.getCity().toUpperCase());
			res.setCity(city);
			
			res.setTimestamp(dao.getEventTime().getTime() / 1000);
			
			ReportsAggregator aggr = new ReportsAggregator();
			aggr.aggregateResult(dao.getPlays(), dao.getAlive(), dao.getBufferTime(), dao.getBitrate(), dao.getBitrateCount());
			aggr.fillObject(res);
			
			result.add(res);
		}
		
		if(specificOrderBy) 
			result = orderBy(filter.getOrderByType(), pageIdx, pageSize, count, result);
		
		return new LiveStatsListResponse(result, count);
	}

	/**
	 * This function orders all the results by plays. Sadly, as cassandara doesn't support order by anything by the PK
	 * we must read all results and order in memory.
	 * @return
	 */
	protected List<LiveStats> orderBy(final LiveReportOrderBy liveReportOrderBy, int pageIdx, int pageSize, int count,
			List<LiveStats> result) {
		if((pageIdx - 1) * pageSize > result.size()) {
			return new ArrayList<LiveStats>(); 
		}
		
		Collections.sort(result, new Comparator<LiveStats>() {

			@Override
			public int compare(LiveStats arg0, LiveStats arg1) {
				if(LiveReportOrderBy.PLAYS_DESC.equals(liveReportOrderBy))
					return (int) (arg1.getPlays() - arg0.getPlays());
				if(LiveReportOrderBy.AUDIENCE_DESC.equals(liveReportOrderBy))
					return (int) (arg1.getAudience() - arg0.getAudience());
				return 0;
			}
		});
		
		result = result.subList((pageIdx - 1) * pageSize, Math.min(pageIdx * pageSize, count));
		return result;
	}

	private int getRecordsCount(LiveReportInputFilter filter) {
		String query = generateQuery(filter, true);
		ResultSet results = session.getSession().execute(query);
		Iterator<Row> itr = results.iterator();
		return (int) itr.next().getLong("count");
	}

	private String generateQuery(LiveReportInputFilter filter, boolean count) {
		
		StringBuffer sb = new StringBuffer();
		sb.append("select ");
		sb.append(count ? "count(*)" : "*");
		sb.append(" from kaltura_live.live_events_location where ");
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

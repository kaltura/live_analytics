package com.kaltura.live.webservice.reporters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
	
	protected class GeoKey {
		protected String entryId;
		protected Coordinate city;
		protected Coordinate country;
		
		public GeoKey(String entryId, Coordinate city, Coordinate country) {
			super();
			this.entryId = entryId;
			this.city = city;
			this.country = country;
		}
		
		public String getEntryId() {
			return entryId;
		}
		public Coordinate getCity() {
			return city;
		}
		public Coordinate getCountry() {
			return country;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((city == null) ? 0 : city.hashCode());
			result = prime * result
					+ ((country == null) ? 0 : country.hashCode());
			result = prime * result
					+ ((entryId == null) ? 0 : entryId.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			GeoKey other = (GeoKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (city == null) {
				if (other.city != null)
					return false;
			} else if (!city.equals(other.city))
				return false;
			if (country == null) {
				if (other.country != null)
					return false;
			} else if (!country.equals(other.country))
				return false;
			if (entryId == null) {
				if (other.entryId != null)
					return false;
			} else if (!entryId.equals(other.entryId))
				return false;
			return true;
		}

		private EntryGeoTimeLineReporter getOuterType() {
			return EntryGeoTimeLineReporter.this;
		}
	}
	
	@Override
	public LiveStatsListResponse query(LiveReportInputFilter filter, LiveReportPager pager) {
		String query = generateQuery(filter);
		ResultSet results = session.getSession().execute(query);
		
		Iterator<Row> itr = results.iterator();
		Map<GeoKey, ReportsAggregator> map = new HashMap<GeoKey, ReportsAggregator>();
		
		// Aggregate result
		while(itr.hasNext()) {
			LiveEntryLocationEventDAO dao = new LiveEntryLocationEventDAO(itr.next());
			
			Coordinate city = GeographicalLocatorsCache.getCityLocator().getCityCoordinates(dao.getCountry(), dao.getCity());
			Coordinate country = GeographicalLocatorsCache.getCountryLocator().getCountryCoordinates(dao.getCountry());
			
			GeoKey key = new GeoKey(dao.getEntryId(), city, country);
			if(!map.containsKey(key)) {
				map.put(key, new ReportsAggregator());
			}
			
			map.get(key).aggregateResult(dao.getPlays(), dao.getAlive(), dao.getBufferTime(), dao.getBitrate(), dao.getBitrateCount());
		}
		
		// Extract final stats
		List<LiveStats> result = new ArrayList<LiveStats>();
		for (Entry<GeoKey, ReportsAggregator> stat : map.entrySet()) {
			GeoTimeLiveStats res = new GeoTimeLiveStats();
			res.setEntryId(stat.getKey().getEntryId());
			res.setCountry(stat.getKey().getCountry());
			res.setCity(stat.getKey().getCity());
			
			stat.getValue().fillObject(res);
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
		if(filter.getFromTime() == 0)
			validation += "From time must be a timestamp ";
		if(filter.getToTime() == 0)
			validation += "To time must be a timestamp ";
		
		if(!validation.isEmpty())
			throw new AnalyticsException("Illegal filter input: " + validation);
		
	}
	
	

}

package com.kaltura.live.webservice.reporters;

import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.infra.utils.LiveConfiguration;
import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveReportPager;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

/**
 * Interface for statistics generation
 */
public abstract class BaseReporter {
	
	protected static Logger logger = LoggerFactory.getLogger(BaseReporter.class);
	
	protected static SerializableSession session;
	
	public BaseReporter() {
		session = new SerializableSession(LiveConfiguration.instance().getCassandraNodeName());
	}
	
	/**
	 * This function validates whether the filter is good enough to be used for this specific report
	 * @throws AnalyticsException 
	 */
	abstract public void validateFilter(LiveReportInputFilter filter) throws AnalyticsException;
	
	/**
	 * Abstract function. Each implementor is supposed to use the filter and return the matching live statistics
	 * @param filter The filter by which the results should be queried
	 * @return The matching live statistics
	 */
	abstract public LiveStatsListResponse query(LiveReportInputFilter filter, LiveReportPager pager);
	
	/** --- Base reporter utils --- */ 
	
	protected String addEntryIdsCondition(String entryIdsStr) {
		String[] entryIds = entryIdsStr.split(",");
		if(entryIds.length == 1)
			return "entry_id = '" + entryIdsStr +"'";
		
		StringBuffer sb = new StringBuffer("entry_id IN (");
		int cnt = entryIds.length - 1;
		for (String entryId : entryIds) {
			sb.append("'" + entryId + "'");
			if(cnt-- != 0)
				sb.append(",");
		}
		sb.append(")");
		return sb.toString();
	}
	
	protected String addTimeRangeCondition(Date fromDate, Date toDate) {
		StringBuffer sb = new StringBuffer();
		sb.append("event_time >= ");
		sb.append(fromDate.getTime());
		sb.append(" and event_time <= ");
		sb.append(toDate.getTime());
		
		return sb.toString();
	}
	
	protected String addRangeCondition(Date fromDate, Date toDate, int secs) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(fromDate);
		
		StringBuffer sb = new StringBuffer();
		sb.append("event_time IN (");
		
		boolean isFirst = true;
		while(!cal.getTime().after(toDate)) {
			if(isFirst)
				isFirst = false;
			else
				sb.append(",");
			sb.append(cal.getTime().getTime());
			cal.add(Calendar.SECOND, secs);
		}
		sb.append(")");
		return sb.toString();
	}
	
	protected String addTimeInRangeCondition(Date fromDate, Date toDate) {
		return addRangeCondition(fromDate, toDate, 10);
	}
	
	protected String addTimeInHourRangeCondition(long fromTime, long toTime) {
		
		Date fromTimeDate = new Date(fromTime * 1000);
		Date roundedDate = DateUtils.roundHourDate(fromTimeDate);
		if (!fromTimeDate.before(roundedDate)) {
			Calendar cal = Calendar.getInstance(); 
			cal.setTime(roundedDate); 
			cal.add(Calendar.HOUR_OF_DAY, 1);
			roundedDate = cal.getTime();
		}
		return addRangeCondition(roundedDate, new Date(toTime * 1000), 60*60);
	}
	
	protected boolean isValidateEntryIds(String entryId) {
		return entryId.matches("^([\\w]+,?)*$");
	}
}

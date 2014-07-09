package com.kaltura.live.webservice.reporters;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.live.Configuration;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.webservice.model.AnalyticsException;
import com.kaltura.live.webservice.model.LiveReportInputFilter;
import com.kaltura.live.webservice.model.LiveStatsListResponse;

/**
 * Interface for statistics generation
 */
public abstract class BaseReporter {
	
	protected static Logger logger = LoggerFactory.getLogger(BaseReporter.class);
	
	private static final int TIME_FRAME_INTERVAL = 30;
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	
	protected static SerializableSession session;
	
	public BaseReporter() {
		session = new SerializableSession(Configuration.NODE_NAME);
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
	abstract public LiveStatsListResponse query(LiveReportInputFilter filter);
	
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
		SimpleDateFormat formatDate = new SimpleDateFormat(DATE_FORMAT);
		StringBuffer sb = new StringBuffer();
		sb.append("event_time >= ");
		sb.append("'" + formatDate.format(fromDate) + "'");
		sb.append(" and event_time <= ");
		sb.append("'" + formatDate.format(toDate) + "'");
		
		return sb.toString();
	}
	
	protected String addHoursBeforeCondition(Date curTime, int hoursBefore) {
		SimpleDateFormat formatDate = new SimpleDateFormat(DATE_FORMAT);
		Calendar cal = Calendar.getInstance();
		cal.setTime(DateUtils.roundHourDate(curTime));
		
		StringBuffer sb = new StringBuffer();
		sb.append("event_time IN (");
		sb.append("'" + formatDate.format(cal.getTime()) + "'");
		
		for(int i = 0 ; i < hoursBefore ; ++i) {
			cal.add(Calendar.HOUR, -hoursBefore);
			Date startTime = DateUtils.roundHourDate(cal.getTime());
			sb.append(",'" + formatDate.format(startTime) + "'");
		}
		sb.append(")");
		return sb.toString();
	}
	
	protected String addNowCondition() {
		// Since we aggregate data, now is not really now but 30 seconds before.
		Calendar cal = DateUtils.getCurrentTime();
		cal.add(Calendar.SECOND, -TIME_FRAME_INTERVAL);
		Date roundTime = DateUtils.roundDate(cal.getTime());
		
		return addExactTimeCondition(roundTime);
	}
	
	protected String addExactTimeCondition(Date curTime) {
		SimpleDateFormat formatDate = new SimpleDateFormat(DATE_FORMAT);
		return "event_time = '" + formatDate.format(curTime) + "'"; 
	}
}

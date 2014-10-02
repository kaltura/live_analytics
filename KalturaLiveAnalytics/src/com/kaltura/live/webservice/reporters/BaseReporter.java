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
	
	private static final int TIME_FRAME_INTERVAL = 60;
	
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
	
	protected String addHoursBeforeCondition(Date curTime, int hoursBefore) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(DateUtils.roundHourDate(curTime));
		
		StringBuffer sb = new StringBuffer();
		sb.append("event_time IN (");
		sb.append(cal.getTime().getTime());
		
		for(int i = 0 ; i < hoursBefore ; ++i) {
			cal.add(Calendar.HOUR, -1);
			Date startTime = DateUtils.roundHourDate(cal.getTime());
			sb.append("," + startTime.getTime());
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
		return "event_time = " + curTime.getTime(); 
	}
	
	protected float calcAverageBufferTime(long bufferTime, long alive) {
		if(alive > 0)
			// Round to 2 decimal points
			// And set it to be average on one minute
			return (float) (Math.round((6 * 100.0 * bufferTime) / alive) / 100.0);
		return 0;
	}
}

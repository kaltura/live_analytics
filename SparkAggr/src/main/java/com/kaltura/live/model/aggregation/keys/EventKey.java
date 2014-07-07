package com.kaltura.live.model.aggregation.keys;

import java.io.Serializable;
import java.util.Date;

import com.kaltura.live.model.aggregation.StatsEvent;

/**
 * This class is a base class for all event keys
 */
public abstract class EventKey implements Serializable {
	
	private static final long serialVersionUID = 5507488723242411488L;
	
	protected Date eventTime;
	
	public EventKey(Date eventTime) {
		this.eventTime = eventTime;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((eventTime == null) ? 0 : eventTime.hashCode());
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
		EventKey other = (EventKey) obj;
		if (eventTime == null) {
			if (other.eventTime != null)
				return false;
		} else if (!eventTime.equals(other.eventTime))
			return false;
		return true;
	}
    
    /**
     * This function updates the stats event so the its key fields will be identical
     * to the key object. 
     */
    public void manipulateStatsEventByKey(StatsEvent statsEvent) {
    	return;
    }

	public Date getEventTime() {
		return eventTime;
	}
    
   
        	
}

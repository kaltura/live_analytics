package com.kaltura.live.model.aggregation.keys;

import java.io.Serializable;

import com.kaltura.live.model.StatsEvent;

/**
 * This class is a base class for all event keys
 */
public abstract class EventKey implements Serializable {
	
	private static final long serialVersionUID = 5507488723242411488L;

	/**
	 * This function is marked as abstract to enforce the implementors to implement it.
	 */
	public abstract int hashCode();

	/**
	 * This function is marked as abstract to enforce the implementors to implement it.
	 */
    public abstract boolean equals(Object obj);
    
    /**
     * This function updates the stats event so the its key fields will be identical
     * to the key object. 
     */
    public void manipulateStatsEventByKey(StatsEvent statsEvent) {
    	return;
    }
        	
}

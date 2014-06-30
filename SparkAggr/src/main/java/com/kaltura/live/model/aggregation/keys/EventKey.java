package com.kaltura.live.model.aggregation.keys;

import java.io.Serializable;

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
        	
}

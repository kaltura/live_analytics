package com.kaltura.live;

import java.io.Serializable;

public abstract class EventKey implements Serializable {
	
	public abstract int hashCode();

    public abstract boolean equals(Object obj);
        	
}

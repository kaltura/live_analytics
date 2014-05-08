package com.kaltura.live;

import java.io.Serializable;

public abstract class LiveEventDAO implements Serializable {

	protected StatsEvent aggrRes;
	protected SerializableSession session;
	
	public LiveEventDAO(SerializableSession session) {
		this.session = session;
	}
	public void init(StatsEvent event) {
		this.aggrRes = event;
	}
	
	public abstract void saveOrUpdate();
}





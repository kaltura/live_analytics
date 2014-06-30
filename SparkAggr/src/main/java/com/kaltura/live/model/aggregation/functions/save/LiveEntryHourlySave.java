package com.kaltura.live.model.aggregation.functions.save;

import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.dao.LiveEntryEventDAO;
import com.kaltura.live.model.aggregation.dao.LiveEventDAO;

public class LiveEntryHourlySave extends LiveEventSave {
	
	private static final long serialVersionUID = 2456382584468986687L;
	
	private static final String TABLE_NAME = "kaltura_live.hourly_live_events";
	
	public LiveEntryHourlySave(SerializableSession session) {
		super(session);
	}
	
	@Override
	protected LiveEventDAO createLiveEventDAO() {
		return new LiveEntryEventDAO(TABLE_NAME);
	}

}

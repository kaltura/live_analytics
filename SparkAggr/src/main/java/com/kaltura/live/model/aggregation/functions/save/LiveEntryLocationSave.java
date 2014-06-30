package com.kaltura.live.model.aggregation.functions.save;

import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.dao.LiveEntryLocationEventDAO;
import com.kaltura.live.model.aggregation.dao.LiveEventDAO;

public class LiveEntryLocationSave extends LiveEventSave {
	
	private static final long serialVersionUID = -5105640123955409065L;

	public LiveEntryLocationSave(SerializableSession session) {
		super(session);
	}
	
	@Override
	protected LiveEventDAO createLiveEventDAO() {
		return new LiveEntryLocationEventDAO();
	}

}

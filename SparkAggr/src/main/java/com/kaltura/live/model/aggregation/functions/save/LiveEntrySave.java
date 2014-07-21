package com.kaltura.live.model.aggregation.functions.save;

import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.dao.LiveEntryEventDAO;
import com.kaltura.live.model.aggregation.dao.LiveEventDAO;

/**
 * Save function wrapping for Live entry
 */
public class LiveEntrySave extends LiveEventSave {
	
	private static final long serialVersionUID = 3189544783672808202L;
	
	private static final String TABLE_NAME = "kaltura_live.live_events";
	
	public LiveEntrySave(SerializableSession session) {
		super(session);
	}
	
	@Override
	protected LiveEventDAO createLiveEventDAO() {
		return new LiveEntryEventDAO(TABLE_NAME, LiveEntryEventDAO.AGGR_TTL);
	}
}

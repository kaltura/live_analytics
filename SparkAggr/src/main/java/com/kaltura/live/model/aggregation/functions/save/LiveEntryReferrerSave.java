package com.kaltura.live.model.aggregation.functions.save;

import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.dao.LiveEntryReferrerEventDAO;
import com.kaltura.live.model.aggregation.dao.LiveEventDAO;

public class LiveEntryReferrerSave extends LiveEventSave {
	
	private static final long serialVersionUID = -3895411924827872374L;
	
	public LiveEntryReferrerSave(SerializableSession session) {
		super(session);
	}
	
	@Override
	protected LiveEventDAO createLiveEventDAO() {
		return new LiveEntryReferrerEventDAO();
	}


}

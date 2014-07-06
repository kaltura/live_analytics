package com.kaltura.live.model.aggregation.functions.save;

import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.dao.LiveEventDAO;
import com.kaltura.live.model.aggregation.dao.PartnerEventDAO;

public class PartnerSave extends LiveEventSave {

	private static final long serialVersionUID = 8274772990033635218L;
	
	private static final String TABLE_NAME = "kaltura_live.live_events_partner";
	
	public PartnerSave(SerializableSession session) {
		super(session);
	}

	@Override
	protected LiveEventDAO createLiveEventDAO() {
		return new PartnerEventDAO(TABLE_NAME);
	}

}

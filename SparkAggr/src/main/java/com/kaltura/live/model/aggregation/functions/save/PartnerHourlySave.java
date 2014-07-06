package com.kaltura.live.model.aggregation.functions.save;

import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.model.aggregation.dao.LiveEventDAO;
import com.kaltura.live.model.aggregation.dao.PartnerEventDAO;

public class PartnerHourlySave extends LiveEventSave {

	private static final long serialVersionUID = 5230447429205620876L;
	
	private static final String TABLE_NAME = "kaltura_live.hourly_live_events_partner";
	
	public PartnerHourlySave(SerializableSession session) {
		super(session);
	}

	@Override
	protected LiveEventDAO createLiveEventDAO() {
		return new PartnerEventDAO(TABLE_NAME);
	}

}

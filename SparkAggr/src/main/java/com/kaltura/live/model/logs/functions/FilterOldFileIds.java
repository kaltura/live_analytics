package com.kaltura.live.model.logs.functions;

import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.function.Function;

import com.kaltura.live.SparkConfiguration;
import com.kaltura.live.infra.utils.DateUtils;

public class FilterOldFileIds implements Function<String, Boolean> {

	private static final long serialVersionUID = -7006214542175672609L;

	@Override
	public Boolean call(String fileId) throws Exception {
		if (fileId.contains(String.valueOf(DateUtils.getCurrentHourInMillis() - TimeUnit.HOURS.toMillis(SparkConfiguration.HOURS_TO_SAVE)))) {
			return false;
		}
		return true;
	}
	

}

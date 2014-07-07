package com.kaltura.live.model.logs.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.SparkConfiguration;
import com.kaltura.live.infra.cache.SerializableSession;


/**
 *	New log file watcher
 */
public class GetNewFileIds extends FlatMapFunction<Long, String> {

	private static final long serialVersionUID = 4626988950546579557L;
	
	/** Maps between hour and file names */
	protected static Map<Long, String> hourFiles = new HashMap<Long, String>();
	
	/** Cassandra DB connection*/
	protected SerializableSession session;
	
	public GetNewFileIds(SerializableSession session) {
		this.session = session;
	}
	
	@Override
	public Iterable<String> call(Long hourTimestamp) throws Exception {
		
	  	// if a new hour was started add its key and remove older hours key.
	  	if (!hourFiles.containsKey(hourTimestamp)) {
	  		hourFiles.put(hourTimestamp, "");
	  		if (hourFiles.containsKey(hourTimestamp - TimeUnit.HOURS.toMillis(SparkConfiguration.HOURS_TO_SAVE)))
	  		{
	  			hourFiles.remove(hourTimestamp - TimeUnit.HOURS.toMillis(SparkConfiguration.HOURS_TO_SAVE));
	  		}
	  	}
	  	
		List<String> allKeys = new ArrayList<String>();
		
		for (Entry<Long, String> hourFile : hourFiles.entrySet()) {
			String query = "SELECT file_id from kaltura_live.log_files where hour_id = "
					+ hourFile.getKey()
					+ " and file_id > '"
					+ hourFile.getValue() + "';";
			
			ResultSet results = session.getSession().execute(query);
			String fileId = hourFile.getValue();
			for (Row row : results) {
				fileId = row.getString("file_id");
				allKeys.add(fileId);
			}

			hourFiles.put(hourFile.getKey(), fileId);
		}

		return allKeys;
	}

}

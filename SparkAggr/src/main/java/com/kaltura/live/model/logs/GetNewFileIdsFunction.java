package com.kaltura.live.model.logs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.cache.SerializableSession;

/**
 *	New log file watcher
 */
public class GetNewFileIdsFunction extends FlatMapFunction<String, String> {

	private static final long serialVersionUID = 4626988950546579557L;
	
	/** Maps between hour and file names */
	protected Map<String, String> hourFiles;
	
	/** Cassandra DB connection*/
	protected SerializableSession session;
	
	public GetNewFileIdsFunction(SerializableSession session, Map<String, String> hourFiles) {
		this.hourFiles = hourFiles;
		this.session = session;
	}
	
	@Override
	public Iterable<String> call(String arg0) throws Exception {
		/*
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH Z");
	  	Calendar cal = Calendar.getInstance();
	  	
	  	String currHourId = dateFormat.format(cal.getTime());
	  	if (!hourFiles.containsKey(currHourId)) {
	  		hourFiles.put(currHourId, "");
	  	}
	  	*/
		List<String> allKeys = new ArrayList<String>();
		
		for (java.util.Map.Entry<String, String> hourFile : hourFiles.entrySet()) {
			String query = "SELECT file_id from kaltura_live.log_files where hour_id = '"
					+ hourFile.getKey()
					+ "' and file_id > '"
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

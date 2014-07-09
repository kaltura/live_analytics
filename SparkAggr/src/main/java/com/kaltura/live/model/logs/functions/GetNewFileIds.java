package com.kaltura.live.model.logs.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.cache.SerializableSession;


/**
 *	New log file watcher
 */
public class GetNewFileIds extends FlatMapFunction<Long, String> {

	private static final long serialVersionUID = 4626988950546579557L;
	
	/** Cassandra DB connection*/
	protected SerializableSession session;
	
	public GetNewFileIds(SerializableSession session) {
		this.session = session;
	}
	
	@Override
	public Iterable<String> call(Long hourTimestamp) throws Exception {
		
		List<String> allKeys = new ArrayList<String>();
		
		String query = "SELECT file_id from kaltura_live.log_files where hour_id = "
					+ hourTimestamp
					+ ";";
		ResultSet results = session.getSession().execute(query);
			
		for (Row row : results) {
			String fileId = row.getString("file_id");
			allKeys.add(fileId);
		}

		return allKeys;
	}

}

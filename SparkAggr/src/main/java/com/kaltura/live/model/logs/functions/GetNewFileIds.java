package com.kaltura.live.model.logs.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.kaltura.live.infra.cache.SerializableSession;

/**
 *	New log file watcher
 */
public class GetNewFileIds implements FlatMapFunction<Long, String> {

	private static final long serialVersionUID = 4626988950546579557L;
	
	private static Logger LOG = LoggerFactory.getLogger(GetNewFileIds.class);
	
	/** Cassandra DB connection*/
	protected SerializableSession session;
	
	public GetNewFileIds(SerializableSession session) {
		this.session = session;
	}
	
	@Override
	public Iterable<String> call(Long hourTimestamp) throws Exception {
		LOG.warn("Start: GetNewFileIds");
		List<String> allKeys = new ArrayList<String>();
		
		String query = "SELECT file_id from kaltura_live.log_files where hour_id = "
					+ hourTimestamp
					+ ";";
		
		try {
			LOG.warn("GetNewFileIds: before query");
			ResultSet results = session.getSession().execute(query);
			LOG.warn("GetNewFileIds: after query");
			
			for (Row row : results) {
				String fileId = row.getString("file_id");
				allKeys.add(fileId);
			}
		} catch (QueryExecutionException ex) {
			LOG.error("Failed to run query: " + query + "\n" + ex.getMessage());
		}
		LOG.warn("End: GetNewFileIds");
		return allKeys;
		
	}

}

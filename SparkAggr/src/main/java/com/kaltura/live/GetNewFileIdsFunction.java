package com.kaltura.live;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class GetNewFileIdsFunction extends FlatMapFunction<String, String> {

	Map<String, String> hourFiles;
	private SerializableSession session;
	
	public GetNewFileIdsFunction(Map<String, String> hourFiles, SerializableSession session) {
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
		for (String hourId : hourFiles.keySet()) {
			String lastFileId = hourFiles.get(hourId);
			String query = "SELECT file_id from kaltura_live.log_files where hour_id = '"+hourId+"' and file_id > '" +lastFileId + "';"; 
			//String q1 = "SELECT id FROM kaltura_live.log_data;";
			
	        ResultSet results = session.getSession().execute(query);
	        String fileId = lastFileId;
	        for (Row row : results) {
	        	 fileId = row.getString("file_id");
	             allKeys.add(fileId);
	        }
	        hourFiles.put(hourId, fileId);
		}
		
        return allKeys;
	}

}

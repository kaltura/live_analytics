package com.kaltura.live;

import java.io.FileInputStream;
import java.nio.ByteBuffer;


import java.util.Date;

import org.apache.commons.io.IOUtils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.infra.cache.SessionsCache;
import com.kaltura.live.infra.utils.DateUtils;

public class RegisterFile {

	private SerializableSession cassandraSession;
	
	private static final int LOGS_TTL = 60 * 60 * 3; 
    

    public RegisterFile(String node) {
        cassandraSession = new SerializableSession(node);
    }
    
    private long getTimeStamp(String fileName) {
    	String[] parts = fileName.split("-");
    	long longUnixSeconds = Long.parseLong(parts[1]);
    	return DateUtils.roundHourDate(new Date(longUnixSeconds * 1000L)).getTime();
    	 
    }
    
    public void insertIntoTable(String key, byte[] data) {
    	long hourKey = getTimeStamp(key);
    	PreparedStatement statement = cassandraSession.getSession().prepare("INSERT INTO kaltura_live.log_files (hour_id,file_id) VALUES (?, ?) USING TTL ?");
        BoundStatement boundStatement = new BoundStatement(statement);
        cassandraSession.getSession().execute(boundStatement.bind(new Date(hourKey),key, LOGS_TTL));
        statement = cassandraSession.getSession().prepare("INSERT INTO kaltura_live.log_data (file_id,data) VALUES (?, ?) USING TTL ?");
        boundStatement = new BoundStatement(statement);
        cassandraSession.getSession().execute(boundStatement.bind(key,ByteBuffer.wrap(data), LOGS_TTL));
        System.out.println("After Insert");
    }

    public byte[] readFromTable(String key) {
        String q1 = "SELECT * FROM kaltura_live.log_data WHERE id = '"+key+"';";

        ResultSet results = cassandraSession.getSession().execute(q1);
        for (Row row : results) {
            ByteBuffer data = row.getBytes("data");
	    byte[] result = new byte[data.remaining()];
	    data.get(result);
	    return result;
        }
        return null;
    }
    
    public static byte[] readFile(String fileName){
    	  
    	  FileInputStream fis = null;
    	  
    	  try {
    	   
    	  fis = new FileInputStream(fileName);
    	  byte[] fileData = IOUtils.toByteArray(fis);
    	  System.out.println("read file");
    	  return fileData;
    	 
    	   
    	  } catch (Exception e1) {
    	  
    		  e1.printStackTrace();
    		  return null;
    	   
    	 }
    }
    
    public void disconnect() {
    	cassandraSession.disconnect();
    }
    
    public static void main(String[] args) {
    	
    	String node = "pa-erans";
    	String fileName = "access_log";
    	String logsFolder = "";
    	
    	if (args.length == 1)
    	{
    		node = args[0];
    	}
    	if (args.length == 3)
    	{
    		node = args[0];
    		logsFolder = args[1] + "/";
    		fileName = args[2];
    		
    	}
    	
		RegisterFile insertFile = new RegisterFile(node);
        
	
    	try {
    		String fileNameNoExt = fileName.substring(0, fileName.length()-3);
			insertFile.insertIntoTable(fileNameNoExt, readFile(logsFolder + fileName));
			insertFile.disconnect();
    	} catch (Exception ex) {
    		ex.printStackTrace();
		} finally {
			insertFile.disconnect();
    	}
		
		/**
		// TODO - remove hack and get file name as argument
		try {
			
			int startTime = 1387121430;
			while (startTime <= 1387125000) {
				
				//String fileName = formatDate.format(c.getTime());
				fileName = Integer.toString(startTime) + "_live_stats";
				insertFile.insertIntoTable(fileName, readFile("/home/dev/orly/" + fileName + ".gz"));
				startTime = startTime + 30;
				Thread.sleep(30*1000);
			}
			
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			insertFile.disconnect();
		}
		*/

	}
}

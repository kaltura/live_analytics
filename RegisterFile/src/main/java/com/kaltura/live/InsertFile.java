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

public class InsertFile {

	private SerializableSession cassandraSession;
    

    public InsertFile(String node) {
        cassandraSession = new SerializableSession(node);
    }
    
    private long getTimeStamp(String fileName) {
    	String[] parts = fileName.split("_");
    	long longUnixSeconds = Long.parseLong(parts[0]);
    	return longUnixSeconds * 1000L; 
    }
    
    public void insertIntoTable(String key, byte[] data) {
    	long hourKey = getTimeStamp(key);
    	PreparedStatement statement = cassandraSession.getSession().prepare("INSERT INTO kaltura_live.log_files (hour_id,file_id) VALUES (?, ?)");
        BoundStatement boundStatement = new BoundStatement(statement);
        cassandraSession.getSession().execute(boundStatement.bind(new Date(hourKey),key));
        statement = cassandraSession.getSession().prepare("INSERT INTO kaltura_live.log_data (id,data) VALUES (?, ?)");
        boundStatement = new BoundStatement(statement);
        cassandraSession.getSession().execute(boundStatement.bind(key,ByteBuffer.wrap(data)));
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
    
    public static void main(String[] args) {
    	
		InsertFile insertFile = new InsertFile("pa-erans");
        
		/*
    	try {
	    	String fileName = args[0];
			insertFile.insertIntoTable(fileName, readFile("/home/dev/orly/" + fileName + ".gz"));
			insertFile.disconnect();
    	} catch (Exception ex) {
    		ex.printStackTrace();
		} finally {
			insertFile.disconnect();
    	}
		*/
		
		// TODO - remove hack and get file name as argument
		try {
			
			int startTime = 1387121430;
			while (startTime <= 1387125000) {
				
				//String fileName = formatDate.format(c.getTime());
				String fileName = Integer.toString(startTime) + "_live_stats";
				insertFile.insertIntoTable(fileName, readFile("/home/dev/orly/" + fileName + ".gz"));
				startTime = startTime + 30;
				Thread.sleep(30*1000);
			}
			
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}
}

package com.kaltura.live;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.io.IOUtils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class InsertFile {

	private Cluster cluster;
    private Session session;

    public InsertFile(String node) {
        connect(node);
    }

    private void connect(String node) {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to %s\n", metadata.getClusterName());
        for (Host host: metadata.getAllHosts()) {
              System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                         host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }
    
    private String getDateStr(String fileName) {
    	String[] parts = fileName.split("_");
    	long longUnixSeconds = Long.parseLong(parts[0]);
    	Date date = new Date(longUnixSeconds*1000L); // *1000 is to convert seconds to milliseconds
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH z"); // the format of your date
    	sdf.setTimeZone(TimeZone.getTimeZone("GMT-5"));
    	String formattedDate = sdf.format(date);
    	return formattedDate;
    }

    public void disconnect()
    {
    	session.shutdown();
	cluster.shutdown();
    }
    
    public void insertIntoTable(String key, byte[] data) {
    	String hourKey = getDateStr(key);
    	PreparedStatement statement = session.prepare("INSERT INTO kaltura_live.log_files (hour_id,file_id) VALUES (?, ?)");
        BoundStatement boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(hourKey,key));
        statement = session.prepare("INSERT INTO kaltura_live.log_data (id,data) VALUES (?, ?)");
        boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(key,ByteBuffer.wrap(data)));
	System.out.println("After Insert");
    }

    public byte[] readFromTable(String key) {
        String q1 = "SELECT * FROM kaltura_live.log_data WHERE id = '"+key+"';";

        ResultSet results = session.execute(q1);
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
		try {
			
			int startTime = 1387121430;
			while (startTime <= 1387125000) {
				
				//String fileName = formatDate.format(c.getTime());
				String fileName = Integer.toString(startTime) + "live_stats";
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
		

	}
}

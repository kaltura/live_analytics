package com.kaltura.live;

import java.nio.ByteBuffer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraConnector {
	private static Cluster cluster;
    public static Session session;
    
    public CassandraConnector(String node) {
    	if (session == null)
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
    
    public void disconnect()
    {
    	session.shutdown();
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
    	

    
    

}

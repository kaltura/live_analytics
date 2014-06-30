package com.kaltura.live.infra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class CassandraConnector {
	
	private static Logger LOG = LoggerFactory.getLogger(CassandraConnector.class);
	
	protected static Cluster cluster;
	protected static Session session;
    
    public CassandraConnector(String node) {
    	if (session == null)
    		connect(node);
    }

    private void connect(String node) {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        session = cluster.connect();
        LOG.debug("Connected to %s\n", metadata.getClusterName());
    }
    
    public void disconnect()
    {
    	session.shutdown();
    	LOG.debug("Disconnect from %s\n", session.getCluster().getClusterName());
    }
    
}

package com.kaltura.live;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class SessionsCache {
	private static Map<String, Session> sessions = new HashMap<String, Session>();
	
	public static Session getSession(String node) {
		if (!sessions.containsKey(node)) 
			connect(node);
		
		return sessions.get(node);
		
	}
	
	private static void connect(String node) {
	        Cluster cluster = Cluster.builder().addContactPoint(node).build();
	        Metadata metadata = cluster.getMetadata();
	        System.out.printf("Connected to %s\n", metadata.getClusterName());
	        
	        sessions.put(node, cluster.connect());
	        
	}
	    
	    
}

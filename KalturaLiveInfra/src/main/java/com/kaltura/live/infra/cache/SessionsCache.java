package com.kaltura.live.infra.cache;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class SessionsCache {
	
	private static Map<String, Cluster> clusters = new HashMap<String, Cluster>();
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
	        
	     // TODO - Discuss with Orly the possibility to connect to a given key-space cluster.connect(key-space)
	        clusters.put(node, cluster);
	        sessions.put(node, cluster.connect());
	}
	    
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		for (Cluster cluster : clusters.values()) {
			cluster.close();
		}
		
		for (Session session : sessions.values()) {
			session.close();
		}
	}
	
	public static void disconnect(String node) {
		if (sessions.containsKey(node)) {
			sessions.get(node).close();
			sessions.remove(node);
		}
	}
	
}

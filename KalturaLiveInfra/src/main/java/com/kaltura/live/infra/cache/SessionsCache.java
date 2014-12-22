package com.kaltura.live.infra.cache;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.kaltura.live.infra.exception.KalturaInternalException;

public class SessionsCache {
	
	private static Map<String, Cluster> clusters = new HashMap<String, Cluster>();
	private static Map<String, Session> sessions = new HashMap<String, Session>();
	
	private static Logger LOG = LoggerFactory.getLogger(SessionsCache.class);

	
	public static Session getSession(String node) throws KalturaInternalException {
		if (!sessions.containsKey(node)) 
			connect(node);
		
		return sessions.get(node);
		
	}
	
	private static void connect(String node) throws KalturaInternalException {
		try {
	        Cluster cluster = Cluster.builder().addContactPoint(node).build();
	        
	     // TODO - Discuss with Orly the possibility to connect to a given key-space cluster.connect(key-space)
	        clusters.put(node, cluster);
	        sessions.put(node, cluster.connect());
		} catch (Exception ex) {
			throw new KalturaInternalException("Failed to connect to host [" + node + "]", ex);
		}
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
		
		if (clusters.containsKey(node)) {
			clusters.get(node).close();
			clusters.remove(node);
		}
	}
	
}

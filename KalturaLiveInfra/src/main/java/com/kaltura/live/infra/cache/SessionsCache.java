package com.kaltura.live.infra.cache;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

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
			String[] nodesArray = node.split(",");

			Cluster cluster = Cluster.builder()
					.addContactPoints(nodesArray)
					.withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
					.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE) )
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
					.build();
	        
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

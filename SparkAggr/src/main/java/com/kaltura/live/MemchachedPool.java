package com.kaltura.live;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import net.spy.memcached.MemcachedClient;


public class MemchachedPool {
	private static Map<String, MemcachedClient> memcaches = new HashMap<String, MemcachedClient>();
	
	public static MemcachedClient getCache(String node) throws IOException {
		if (!memcaches.containsKey(node)) 
			connect(node);
		
		return memcaches.get(node);
		
	}
	
	private static void connect(String node) throws IOException {
	        
	        memcaches.put(node,new MemcachedClient(new InetSocketAddress(node, 11211)));
	        
	}
	
	
	    
	    
}

package com.kaltura.live;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.spy.memcached.MemcachedClient;

import com.datastax.driver.core.Session;

public class SerializableMemcache implements Externalizable {
	
	private String node;
	private MemcachedClient memcache;
	
	public SerializableMemcache() {
		node = "test";
	}
	public SerializableMemcache(String node) throws IOException {
		this.node = node;
		memcache = MemchachedPool.getCache(node);
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(node);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		node = (String) in.readObject();		
		memcache = MemchachedPool.getCache(node);
		
	}
	
	public MemcachedClient getCache() {
		return memcache;
	}
	

}

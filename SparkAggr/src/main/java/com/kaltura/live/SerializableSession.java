package com.kaltura.live;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.datastax.driver.core.Session;

public class SerializableSession implements Externalizable {
	
	private String node;
	private Session session;
	
	public SerializableSession() {
		node = "test";
	}
	public SerializableSession(String node) {
		this.node = node;
		session = SessionsCache.getSession(node);
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(node);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		node = (String) in.readObject();		
		session = SessionsCache.getSession(node);
		
	}
	
	public Session getSession() {
		return session;
	}
	

}

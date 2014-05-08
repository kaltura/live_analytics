package com.kaltura.live;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SerializableIP2LocationReader implements Externalizable {

	private String fileName;
	private Ip2LocationReader reader;

	private static Logger LOG = LoggerFactory.getLogger(SerializableIP2LocationReader.class);
	
	public SerializableIP2LocationReader() {
		fileName = "test";
	}
	
	public SerializableIP2LocationReader(String fileName) throws IOException {
		this.fileName = fileName;
		reader = new Ip2LocationReader();
		reader.init(fileName);
	}
	
	public void close() {
		try {
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		fileName = (String) in.readObject();		
		reader = new Ip2LocationReader();
		reader.init(fileName);
		
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(fileName);
		
	}
	
	public Ip2LocationRecord getAll(String ip) throws IOException {
		return reader.getAll(ip);
	}
	

}

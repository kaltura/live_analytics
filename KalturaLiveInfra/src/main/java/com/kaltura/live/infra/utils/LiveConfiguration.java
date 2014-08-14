package com.kaltura.live.infra.utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class LiveConfiguration {
	
	static private LiveConfiguration _instance = null;
	
	private String repositoryHome;

	private String cassandraNodeName;
	
	private String sparkHome;
	
	private String sparkMaster;
	
	private String sparkParallelism; 
	
	private String sparkMaxCores;
	
	private String sparkExectorMem;

	private int hoursToSave;
	
	private int minutesToSave;
	
	private String ip2locationPath;
	
	protected LiveConfiguration(){
	    try{
	    	String confPath = "/opt/kaltura";
	    	if (System.getenv().containsKey("KALTURA_CONF_PATH")) {
	    		confPath = System.getenv().get("KALTURA_CONF_PATH");
	    	}
	    	
	    	InputStream file = new FileInputStream(confPath + "/config.properties") ;
	        Properties props = new Properties();
	        props.load(file);
	        
	        cassandraNodeName = props.getProperty("cassandra.node_name");
	        sparkHome = props.getProperty("spark.home");
	        sparkMaster = props.getProperty("spark.master");
	        sparkParallelism = props.getProperty("spark.parallelism");
	        sparkMaxCores = props.getProperty("spark.max_core");
	        sparkExectorMem = props.getProperty("spark.executor_memory");
	        hoursToSave = Integer.valueOf(props.getProperty("aggr.hours_to_save"));
	        minutesToSave = Integer.valueOf(props.getProperty("aggr.minutes_to_save"));
	        ip2locationPath = props.getProperty("aggr.ip2location_path");
	        	        
	    } 
	    catch(Exception e){
	        System.out.println("error" + e);
	    }	 
	}
		 
    static public LiveConfiguration instance(){
        if (_instance == null) {
            _instance = new LiveConfiguration();
        }
        return _instance;
    }
	    
    public String getRepositoryHome() {
		return repositoryHome;
	}

	public String getCassandraNodeName() {
		return cassandraNodeName;
	}
	
	public String getSparkHome() {
		return sparkHome;
	}

	public String getSparkMaster() {
		return sparkMaster;
	}

	public String getSparkParallelism() {
		return sparkParallelism;
	}

	public String getSparkMaxCores() {
		return sparkMaxCores;
	}

	public String getSparkExectorMem() {
		return sparkExectorMem;
	}

	public int getHoursToSave() {
		return hoursToSave;
	}

	public int getMinutesToSave() {
		return minutesToSave;
	}

	public String getIp2locationPath() {
		return ip2locationPath;
	}


}

package com.kaltura.live.infra.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(RequestUtils.class);

	public static Map<String, String> splitQuery(String query)  {
	    Map<String, String> query_pairs = new LinkedHashMap<String, String>();
	    
	    String[] pairs = query.split("&");
	    for (String pair : pairs) {
	        int idx = pair.indexOf("=");
	        try {
				query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				LOG.error("Failed to parse request", e);
			}
	    }
	    return query_pairs;
	}
}

package com.kaltura.live.model.logs.functions;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.infra.utils.LiveConfiguration;

public class LoadNewFiles implements FlatMapFunction<String, String> {

	private static final long serialVersionUID = 1673487538347695847L;
	
	private static Logger LOG = LoggerFactory.getLogger(LoadNewFiles.class);

	SerializableSession session = new SerializableSession(
			LiveConfiguration.instance().getCassandraNodeName());

	@Override
	public Iterable<String> call(String fileId) {
		List<String> lines = new ArrayList<String>();

		byte[] fileData = null;
		
		String q1 = "SELECT * FROM kaltura_live.log_data WHERE file_id = '"
				+ fileId + "';";

		ResultSet results = session.getSession()
				.execute(q1);
		for (Row row : results) {
			ByteBuffer data = row.getBytes("data");
			byte[] result = new byte[data.remaining()];
			data.get(result);
			fileData = result;
		}

		if (fileData != null)
		{
			ByteArrayInputStream bStream = new ByteArrayInputStream(
					fileData);
			
			GZIPInputStream gzis = null;
			BufferedReader in = null;
			try {
				gzis = new GZIPInputStream(
						bStream);
				InputStreamReader reader = new InputStreamReader(
						gzis);
				in = new BufferedReader(reader);
				String readed;
				while ((readed = in.readLine()) != null) {
					lines.add(readed);
				}
			} catch (IOException e) {
				
			} finally {
				try {
					if (bStream != null)
						bStream.close();
					if (gzis != null)
						gzis.close();
					if (in != null)
						in.close();
				}catch (IOException ex) {
					LOG.error("Failed to close GZipInputStream" + ex.getMessage());
	            }
			}
		}
		return lines;
	}
}


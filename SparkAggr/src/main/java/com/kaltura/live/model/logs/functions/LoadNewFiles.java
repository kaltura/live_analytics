package com.kaltura.live.model.logs.functions;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.SparkConfiguration;
import com.kaltura.live.infra.cache.SerializableSession;

public class LoadNewFiles extends FlatMapFunction<String, String> {

	private static final long serialVersionUID = 1673487538347695847L;

	SerializableSession session = new SerializableSession(
			SparkConfiguration.NODE_NAME);

	@Override
	public Iterable<String> call(String fileId) throws Exception {
		List<String> lines = new ArrayList<String>();

		byte[] fileData = null;
		
		String q1 = "SELECT * FROM kaltura_live.log_data WHERE id = '"
				+ fileId + "';";

		ResultSet results = session.getSession()
				.execute(q1);
		for (Row row : results) {
			ByteBuffer data = row.getBytes("data");
			byte[] result = new byte[data.remaining()];
			data.get(result);
			fileData = result;
		}

		ByteArrayInputStream bStream = new ByteArrayInputStream(
				fileData);
		GZIPInputStream gzis = new GZIPInputStream(
				bStream);
		InputStreamReader reader = new InputStreamReader(
				gzis);
		BufferedReader in = new BufferedReader(reader);
		String readed;
		while ((readed = in.readLine()) != null) {
			lines.add(readed);
		}
		
		return lines;
	}

}


package com.kaltura.live;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kaltura.live.infra.cache.SerializableMemcache;
import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.ip2location.SerializableIP2LocationReader;
import com.kaltura.live.model.StatsEvent;
import com.kaltura.live.model.aggregation.functions.map.LiveEntryHourlyMap;
import com.kaltura.live.model.aggregation.functions.map.LiveEntryLocationMap;
import com.kaltura.live.model.aggregation.functions.map.LiveEntryMap;
import com.kaltura.live.model.aggregation.functions.map.LiveEntryReferrerMap;
import com.kaltura.live.model.aggregation.functions.reduce.LiveEventReduce;
import com.kaltura.live.model.aggregation.functions.save.LiveEntryHourlySave;
import com.kaltura.live.model.aggregation.functions.save.LiveEntryLocationSave;
import com.kaltura.live.model.aggregation.functions.save.LiveEntryReferrerSave;
import com.kaltura.live.model.aggregation.functions.save.LiveEntrySave;
import com.kaltura.live.model.aggregation.threads.HourlyLiveAggregationThread;
import com.kaltura.live.model.aggregation.threads.LiveAggregationThread;
import com.kaltura.live.model.logs.GetNewFileIdsFunction;

/**
 * This class is the main class responsible for the spark aggregation
 * That's a temporal class used to test the system and will be replaced. one day.
 */
public class SparkAggregation {

	private static Logger LOG = LoggerFactory.getLogger(SparkAggregation.class);

	public static void main(String[] args) throws Exception {

		validateArguments(args);
		final JavaSparkContext jsc = initializeEnvironment();

		SerializableSession session = new SerializableSession(SparkConfiguration.NODE_NAME);
		
		// Generate aggregation threads
		LiveAggregationThread entryAggr = new LiveAggregationThread(new LiveEntryMap(), new LiveEventReduce(), new LiveEntrySave(session));
		HourlyLiveAggregationThread entryHourlyAggr = new HourlyLiveAggregationThread(new LiveEntryHourlyMap(), new LiveEventReduce(), new LiveEntryHourlySave(session));
		LiveAggregationThread locationEntryAggr = new LiveAggregationThread(new LiveEntryLocationMap(), new LiveEventReduce(), new LiveEntryLocationSave(session));
		HourlyLiveAggregationThread referrerHourlyAggr = new HourlyLiveAggregationThread(new LiveEntryReferrerMap(), new LiveEventReduce(), new LiveEntryReferrerSave(session));

		
		Map<String, String> filesByHour = new HashMap<String, String>();
		filesByHour.put("2013-12-15 10 GMT-05:00", "");
		filesByHour.put("2013-12-15 11 GMT-05:00", "");

		JavaRDD<String> loadedDates = null;
		boolean resume = true;
		while (resume) {

			List<String> a = new ArrayList<String>();
			a.add("live_events");
			JavaRDD<String> dates = jsc.parallelize(a, 8);

			dates = dates.flatMap(new GetNewFileIdsFunction(session,filesByHour));

			/*
			 * dates = dates.flatMap(new FlatMapFunction<String, String>() {
			 * 
			 * public Iterable<String> call(String s) { SerializableSession
			 * session = new SerializableSession("pa-erans"); String query =
			 * "SELECT file_id from kaltura_live.log_files where hour_id = '"
			 * +hourId+"' and fileId > '" +lastFileId + "';"; String q1 =
			 * "SELECT id FROM kaltura_live.log_data;"; List<String> allKeys =
			 * new ArrayList<String>(); ResultSet results =
			 * session.getSession().execute(q1); for (Row row : results) {
			 * allKeys.add(row.getString("id")); } return allKeys;
			 * 
			 * }
			 * 
			 * });
			 * 
			 * 
			 * JavaRDD<String> inRangeDates = dates.filter(new Function<String,
			 * Boolean>() { // filter only file names in time range public
			 * Boolean call(String date) { return true; } });
			 */

			JavaRDD<String> datesToLoad = dates;
			// get new file names to load
			if (loadedDates != null) {
				datesToLoad = dates.subtract(loadedDates);

			}

			LOG.info("Before dates to load count");
			if (datesToLoad.count() > 0) {
				long startTime = System.currentTimeMillis();
				System.out.println("Start: " + startTime);
				// load new files
				// need to repartitions according to the number of new files to
				// load
				JavaRDD<String> loadedLines = datesToLoad
						.flatMap(new FlatMapFunction<String, String>() {
							SerializableSession session = new SerializableSession(
									SparkConfiguration.NODE_NAME);

							public Iterable<String> call(String fileId)
									throws Exception {
								List<String> lines = new ArrayList<String>();

								byte[] fileData = null;
								LOG.info("Before loading file" + fileId);
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
								LOG.info("After loading file" + fileId);
								return lines;
							}

						});

				if (loadedDates != null) {
					datesToLoad = datesToLoad.union(loadedDates).coalesce(8);
					loadedDates.unpersist();
				}
				loadedDates = datesToLoad;
				loadedDates.cache();
				loadedDates.count();

				// Map each line events to statsEvent object
				JavaRDD<StatsEvent> loadedEvents = loadedLines
						.repartition(24)
						.mapPartitions(
								new FlatMapFunction<Iterator<String>, StatsEvent>() {
									// SerializableIP2LocationReader reader =
									// new
									// SerializableIP2LocationReader("/opt/kaltura/data/geoip/IP-COUNTRY-ISP.BIN");

									public Iterable<StatsEvent> call(
											Iterator<String> it)
											throws Exception {
										SerializableIP2LocationReader reader = new SerializableIP2LocationReader(
												"/opt/kaltura/data/geoip/IP-COUNTRY-ISP.BIN");
										SerializableMemcache memcache = new SerializableMemcache(
												"pa-erans");
										LOG.info("Start mapPartitions");
										List<StatsEvent> statsEvents = new ArrayList<StatsEvent>();
										while (it.hasNext()) {
											String line = it.next();
											statsEvents.add(new StatsEvent(
													line, reader, memcache));
										}
										// reader.close();
										reader.close();

										LOG.info("Stop mapPartitions");
										return statsEvents;
									}
								});

				/*
				 * //Union relevant events from cache with new loaded events
				 * JavaPairRDD<String, StatsEvent> allEvents;
				 * 
				 * if (currDateEvents != null) { allEvents =
				 * currDateEvents.filter(new Function<Tuple2<String,
				 * StatsEvent>, Boolean>() { public Boolean call(Tuple2<String,
				 * StatsEvent> event) { return true; }
				 * }).union(loadedEvents).coalesce(8); } else { allEvents =
				 * loadedEvents; }
				 */

				loadedEvents.cache();
				loadedEvents.count();

				// events.cache();
				// events.count();

				// Run each aggregation in different thread
				List<Thread> aggregations = new ArrayList<Thread>();

				entryAggr.init(loadedEvents);
				entryHourlyAggr.init(loadedEvents);
				locationEntryAggr.init(loadedEvents);
				referrerHourlyAggr.init(loadedEvents);

				aggregations.add(new Thread(entryAggr));
				aggregations.add(new Thread(entryHourlyAggr));
				aggregations.add(new Thread(locationEntryAggr));
				aggregations.add(new Thread(referrerHourlyAggr));

				for (Thread aggr : aggregations) {
					aggr.start();
				}

				for (Thread aggr : aggregations) {
					aggr.join();
				}

				loadedEvents.unpersist();

				long endTime = System.currentTimeMillis();
				System.out.println("Iteration time (msec): "
						+ (endTime - startTime));
				Thread.sleep(1000 * 20);

				// Thread.sleep(1000 * 120);
				// System.exit(0);

				JavaRDD<String> exit = dates
						.filter(new Function<String, Boolean>() {
							// filter only file names in time range
							public Boolean call(String date) {
								if (date.equals("20131215103200"))
									return true;
								return false;
							}
						});

				if (exit.count() == 1)
					resume = false;

			}

		}
		//
		Thread.sleep(1000 * 120);
		System.exit(0);

	}

	private static JavaSparkContext initializeEnvironment() {
		System.setProperty("spark.default.parallelism", SparkConfiguration.PARALLELISM);
		System.setProperty("spark.cores.max", SparkConfiguration.MAX_CORES);
		String[] jars = { SparkConfiguration.REPOSITORY_HOME + "/target/spark-aggr-1.0.jar",
				SparkConfiguration.REPOSITORY_HOME + "/lib/cassandra-driver-core-2.0.0-rc2.jar" };
		final JavaSparkContext jsc = new JavaSparkContext("local[24]",
				"SparkAggr", "/opt/spark/spark-0.8.1-incubating/", jars);
		return jsc;
	}

	private static void validateArguments(String[] args) {
		if (args.length > 1) {
			System.err.println("Usage: SparkAggr <master>");
			System.exit(1);
		}
	}

}

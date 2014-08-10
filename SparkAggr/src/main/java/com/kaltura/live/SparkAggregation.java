package com.kaltura.live;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.live.infra.cache.SerializableSession;
import com.kaltura.live.infra.utils.DateUtils;
import com.kaltura.live.model.aggregation.StatsEvent;
import com.kaltura.live.model.aggregation.functions.map.LiveEntryHourlyMap;
import com.kaltura.live.model.aggregation.functions.map.LiveEntryLocationMap;
import com.kaltura.live.model.aggregation.functions.map.LiveEntryMap;
import com.kaltura.live.model.aggregation.functions.map.LiveEntryReferrerMap;
import com.kaltura.live.model.aggregation.functions.map.PartnerHourlyMap;
import com.kaltura.live.model.aggregation.functions.map.StatsEventMap;
import com.kaltura.live.model.aggregation.functions.reduce.LiveEventReduce;
import com.kaltura.live.model.aggregation.functions.save.LiveEntryHourlySave;
import com.kaltura.live.model.aggregation.functions.save.LiveEntryLocationSave;
import com.kaltura.live.model.aggregation.functions.save.LiveEntryReferrerSave;
import com.kaltura.live.model.aggregation.functions.save.LiveEntrySave;
import com.kaltura.live.model.aggregation.functions.save.PartnerHourlySave;
import com.kaltura.live.model.aggregation.threads.HourlyLiveAggregationCycle;
import com.kaltura.live.model.aggregation.threads.LiveAggregationCycle;
import com.kaltura.live.model.aggregation.threads.RealTimeLiveAggregationCycle;
import com.kaltura.live.model.logs.functions.FilterOldFileIds;
import com.kaltura.live.model.logs.functions.GetNewFileIds;
import com.kaltura.live.model.logs.functions.LoadNewFiles;

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
		LiveAggregationCycle entryAggr = new RealTimeLiveAggregationCycle(new LiveEntryMap(), new LiveEventReduce(), new LiveEntrySave(session));
		HourlyLiveAggregationCycle entryHourlyAggr = new HourlyLiveAggregationCycle(new LiveEntryHourlyMap(), new LiveEventReduce(), new LiveEntryHourlySave(session));
		LiveAggregationCycle locationEntryAggr = new RealTimeLiveAggregationCycle(new LiveEntryLocationMap(), new LiveEventReduce(), new LiveEntryLocationSave(session));
		HourlyLiveAggregationCycle referrerHourlyAggr = new HourlyLiveAggregationCycle(new LiveEntryReferrerMap(), new LiveEventReduce(), new LiveEntryReferrerSave(session));
		HourlyLiveAggregationCycle partnerHourlyAggr = new HourlyLiveAggregationCycle(new PartnerHourlyMap(), new LiveEventReduce(), new PartnerHourlySave(session));
		
		long executionStartTime = System.currentTimeMillis();

		JavaRDD<String> loadedDates = null;
		Set<Long> hoursToLoad = new HashSet<Long>();
		boolean resume = true;
		while (resume) {
			
			hoursToLoad.add(DateUtils.getCurrentHourInMillis(executionStartTime));
			List<Long> hoursToLoadList = new ArrayList<Long>();
			hoursToLoadList.addAll(hoursToLoad);
			JavaRDD<Long> dates = jsc.parallelize(hoursToLoadList, 8);
			
			JavaRDD<String> fileIdsToLoad = dates.flatMap(new GetNewFileIds(session));

			// get new file names to load
			if (loadedDates != null) {
				fileIdsToLoad = fileIdsToLoad.subtract(loadedDates);
			}

			// if new files were added
			if (fileIdsToLoad.count() > 0) {
				long startTime = System.currentTimeMillis();
				LOG.debug("Start: " + startTime);
				// load new files
				// need to repartitions according to the number of new files to load
				JavaRDD<String> loadedLines = fileIdsToLoad.flatMap(new LoadNewFiles());
				
				if (loadedDates != null) {
					fileIdsToLoad = fileIdsToLoad.union(loadedDates).coalesce(8);
					loadedDates.unpersist();
					fileIdsToLoad.filter(new FilterOldFileIds());
				}
				
				loadedDates = fileIdsToLoad;
				loadedDates.cache();
				loadedDates.count();

				// Map each line events to statsEvent object
				JavaRDD<StatsEvent> loadedEvents = loadedLines
						.repartition(24)
						.mapPartitions(
								new StatsEventMap());
				
				loadedEvents.cache();
				loadedEvents.count();

				entryAggr.init(loadedEvents);
				entryHourlyAggr.init(loadedEvents);
				locationEntryAggr.init(loadedEvents);
				referrerHourlyAggr.init(loadedEvents);
				partnerHourlyAggr.init(loadedEvents);
				
				entryAggr.run();
				entryHourlyAggr.run();
				locationEntryAggr.run();
				referrerHourlyAggr.run();
				partnerHourlyAggr.run();
				
				loadedEvents.unpersist();

				long endTime = System.currentTimeMillis();
				System.out.println("Iteration time (msec): "
						+ (endTime - startTime));
				Thread.sleep(1000 * 20);
			}

		}
		
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

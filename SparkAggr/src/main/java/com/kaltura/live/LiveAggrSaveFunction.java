package com.kaltura.live;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

import scala.Tuple2;

public abstract class LiveAggrSaveFunction extends FlatMapFunction<Iterator<Tuple2<EventKey, StatsEvent>>, Boolean> {
	
}
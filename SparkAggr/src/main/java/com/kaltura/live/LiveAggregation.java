package com.kaltura.live;

import org.apache.spark.api.java.function.PairFunction;


public abstract class LiveAggregation extends PairFunction<StatsEvent, EventKey, StatsEvent> {
    
}
